"""Require declared node separation and record the observed experiment placement."""
import argparse
import copy
import json
from pathlib import Path

from placement_control import PlacementMismatch, pod_identity, validate_pod_reference

HOST = 'kubernetes.io/hostname'
APPS = ('producer-sts', 'consumer-sts')
BROKER_LABEL = {'app.kubernetes.io/name': 'kafka'}


def policy(config):
    value = str(config.get('SEPARATE_BROKER_NODES', 'false')).lower()
    if value not in ('true', 'false'):
        raise ValueError('SEPARATE_BROKER_NODES must be true or false')
    return dict(version=1, separate_producers_and_consumers=True,
                separate_brokers=value == 'true', topology_key=HOST,
                broker_label=BROKER_LABEL.copy(), namespace_scope='same_namespace')


def separation_term(brokers=False):
    selector = (dict(matchLabels=BROKER_LABEL.copy()) if brokers else
                dict(matchExpressions=[dict(key='app', operator='In', values=list(APPS))]))
    return dict(topologyKey=HOST, labelSelector=selector)


def has_required_term(spec, namespace, brokers=False):
    terms = spec.get('affinity', {}).get('podAntiAffinity', {}).get(
        'requiredDuringSchedulingIgnoredDuringExecution', [])
    for term in terms:
        # Limit this experiment to its namespace; extra selector keys can narrow the rule.
        if (term.get('topologyKey') != HOST or term.get('namespaceSelector') is not None or
                term.get('namespaces') not in (None, [], [namespace]) or
                term.get('matchLabelKeys') or term.get('mismatchLabelKeys')):
            continue
        selector = term.get('labelSelector', {})
        if brokers:
            if selector == dict(matchLabels=BROKER_LABEL):
                return True
        elif set(selector) == {'matchExpressions'}:
            expressions = selector['matchExpressions']
            if (len(expressions) == 1 and expressions[0].get('key') == 'app' and
                    expressions[0].get('operator') == 'In' and
                    set(expressions[0].get('values', [])) >= set(APPS)):
                return True
    return False


def template_issues(statefulsets, namespace, rules):
    issues = []
    by_name = {item.get('metadata', {}).get('name'): item for item in statefulsets}
    for app in APPS:
        item = by_name.get(app, {})
        spec = item.get('spec', {})
        template = spec.get('template', {})
        if (item.get('metadata', {}).get('namespace') != namespace or
                template.get('metadata', {}).get('labels', {}).get('app') != app or
                spec.get('selector', {}).get('matchLabels', {}).get('app') != app):
            issues.append(app + ': missing StatefulSet or inconsistent namespace/app label')
        if not has_required_term(template.get('spec', {}), namespace):
            issues.append(app + ': template lacks required producer/consumer anti-affinity')
        if rules['separate_brokers'] != has_required_term(template.get('spec', {}), namespace, True):
            issues.append(app + ': required broker rule differs from SEPARATE_BROKER_NODES')
    return issues


def observe(items, expected, namespace, rules, initial=None):
    """Return an auditable result; callers decide whether to wait or fail the run."""
    report = dict(valid=False, issues=[], pods=[], namespace_colocations=[])
    issues = report['issues']
    pipeline = []
    active = []
    for item in items:
        meta = item.get('metadata', {})
        if meta.get('namespace') != namespace:
            issues.append('Unexpected namespace in pod observation: ' + str(meta.get('name')))
            continue
        labels = meta.get('labels', {})
        owned = any(o.get('kind') == 'StatefulSet' and o.get('name') in APPS
                    for o in meta.get('ownerReferences', []))
        if meta.get('name') in expected or labels.get('app') in APPS or owned:
            pipeline.append(item)
        if item.get('status', {}).get('phase') not in ('Succeeded', 'Failed'):
            active.append(item)
    names = [p['metadata'].get('name') for p in pipeline]
    if len(names) != len(set(names)) or set(names) != set(expected):
        issues.append('Pod membership differs: expected ' + ', '.join(sorted(expected)) +
                      '; observed ' + ', '.join(sorted(str(n) for n in names)))
    for item in pipeline:
        meta, spec = item['metadata'], item.get('spec', {})
        name = meta.get('name', '?')
        if meta.get('labels', {}).get('app') != expected.get(name, '') + '-sts':
            issues.append(name + ': unexpected role label')
        if not has_required_term(spec, namespace):
            issues.append(name + ': pod lacks required producer/consumer anti-affinity; rollout needed')
        if rules['separate_brokers'] != has_required_term(spec, namespace, True):
            issues.append(name + ': required broker rule differs from SEPARATE_BROKER_NODES')
        try:
            report['pods'].append(pod_identity(item))
        except PlacementMismatch as exc:
            issues.append(str(exc))
        for condition in item.get('status', {}).get('conditions', []):
            if condition.get('type') == 'PodScheduled' and condition.get('status') == 'False':
                issues.append(name + ': ' + condition.get('reason', 'unscheduled') + ': ' + condition.get('message', ''))
    try:
        validate_pod_reference(report['pods'])
    except ValueError as exc:
        issues.append(str(exc))
    current = {p['pod']: p for p in report['pods']}
    for old in initial or []:
        if current.get(old['pod']) != old:
            issues.append(old['pod'] + ': original pod/node/container identity or resources changed')
    by_node = {}
    for p in report['pods']:
        if p['node']:
            by_node.setdefault(p['node'], []).append(p['pod'])
    for node, names in by_node.items():
        if len(names) > 1:
            issues.append('Producer/consumer pods share node ' + node + ': ' + ', '.join(sorted(names)))
    brokers = [p for p in active if all(p.get('metadata', {}).get('labels', {}).get(k) == v
                                      for k, v in BROKER_LABEL.items())]
    if rules['separate_brokers'] and not brokers:
        issues.append('Broker separation requested but no matching Kafka pods were observed in this namespace')
    for item in active:
        meta, spec = item.get('metadata', {}), item.get('spec', {})
        if meta.get('name') in expected or spec.get('nodeName') not in by_node:
            continue
        broker = all(meta.get('labels', {}).get(k) == v for k, v in BROKER_LABEL.items())
        report['namespace_colocations'].append(dict(pod=meta.get('name'), node=spec['nodeName'],
            experiment_pods=sorted(by_node[spec['nodeName']]), kafka_label_matches=broker,
            phase=item.get('status', {}).get('phase')))
        if broker and rules['separate_brokers']:
            issues.append('Kafka pod ' + meta['name'] + ' shares experiment node ' + spec['nodeName'])
    report['broker_pods_observed'] = [p['metadata']['name'] for p in brokers]
    report['valid'] = not issues
    return report


def verify(api, control, stage, consumers=None):
    """Check live templates and pods at a boundary, preserving failures in the run."""
    import time
    started = time.monotonic()
    rules = policy(control['config'])
    names = control['consumer_pods'] if consumers is None else consumers
    expected = {name: role for role, members in [('producer', control['producer_pods']), ('consumer', names)]
                for name in members}
    record = dict(stage=stage, valid=False, timestamp=time.time(), namespace=api.NS, policy=rules)
    try:
        if control.get('node_placement_policy', rules) != rules:
            raise PlacementMismatch('Node placement policy changed during the run')
        sets = json.loads(api.kubectl('get', 'statefulset', *APPS, '-o', 'json', timeout=10))['items']
        items = json.loads(api.kubectl('get', 'pods', '-o', 'json', timeout=10))['items']
        record['expected_pods'] = sorted(expected)
        record.update(observe(items, expected, api.NS, rules, control.get('initial_node_placement')))
        record['templates'] = {s['metadata']['name']: dict(
            uid=s['metadata'].get('uid'), generation=s['metadata'].get('generation'),
            labels=s.get('spec', {}).get('template', {}).get('metadata', {}).get('labels'),
            affinity=s.get('spec', {}).get('template', {}).get('spec', {}).get('affinity')) for s in sets}
        record['issues'].extend(template_issues(sets, api.NS, rules))
        record['observation_seconds'] = time.monotonic() - started
        if record['observation_seconds'] > 30:
            record['issues'].append('Node placement observation exceeded the 30-second freshness budget')
        record['valid'] = not record['issues']
        if not record['valid']:
            raise PlacementMismatch('; '.join(record['issues']))
        if stage == 'before_production':
            control['node_placement_policy'] = rules
            control['initial_node_placement'] = copy.deepcopy(record['pods'])
        return record
    except Exception as exc:
        record.update(valid=False, error=str(exc))
        raise
    finally:
        record['timestamp'] = time.time()
        api.journal(control, 'node-placement-checks.jsonl', record)


def render(path, separate_brokers=False):
    import yaml
    item = yaml.safe_load(Path(path).read_text())
    spec = item['spec']['template']['spec']
    required = spec.setdefault('affinity', {}).setdefault('podAntiAffinity', {}).setdefault(
        'requiredDuringSchedulingIgnoredDuringExecution', [])
    if separate_brokers:
        required.append(separation_term(True))
    issues = template_issues([item], item['metadata']['namespace'],
                            policy({'SEPARATE_BROKER_NODES': str(separate_brokers)}))
    # Validate only the requested role; deploy-sts applies both roles separately.
    own = item['metadata']['name']
    problems = [s for s in issues if s.startswith(own + ':')]
    if own not in APPS or problems:
        raise ValueError('; '.join(problems) or 'Expected a producer/consumer StatefulSet')
    return item


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Render a producer/consumer manifest without deploying it.')
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--separate-brokers', action='store_true')
    args = parser.parse_args()
    print(json.dumps(render(args.manifest, args.separate_brokers), indent=2))
