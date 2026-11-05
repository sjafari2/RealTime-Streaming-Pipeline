"""Compare four and twelve hot partitions on one fixed deployment at 600 msg/s."""
import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import yaml

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'my-shell'))
from placement_control import pod_identity
from comparison_admission import AdmissionStopped, check_saved_control, require_passed

ARMS = ['keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6']
STRESS = dict(window_seconds=60, consecutive_windows=3,
              minimum_growth_offsets_per_second=100, minimum_mean_backlog_offsets=10000,
              minimum_coverage=.95, maximum_gap_seconds=3)
BALANCE = dict(maximum_mean_backlog=500, maximum_growth=1,
               minimum_coverage=.95, input_rate_tolerance=.01)
ANCHOR_LABEL = 'stream-study-placement'
ANCHOR_VALUE = 'matched-skew600-20261104'
INDEX = 'apps.kubernetes.io/pod-index'


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def trial_order():
    first = [(f'hot{n}__{arm}', 1, 81) for arm in ARMS for n in (12, 4)]
    return first + [(arm, 2, 82) for arm, _, _ in reversed(first)]


def split_arm(arm):
    if '__' not in arm:
        return 12, arm
    block, response = arm.split('__', 1)
    count = int(block.removeprefix('hot')) if hasattr(str, 'removeprefix') else int(block[3:])
    if count not in (4, 12) or response not in ARMS:
        raise ValueError('Unreviewed response or hot-partition count')
    return count, response


def broker_identity(pod):
    if pod['metadata'].get('deletionTimestamp') or not any(
            c.get('type') == 'Ready' and c.get('status') == 'True'
            for c in pod.get('status', {}).get('conditions', [])):
        raise RuntimeError('Broker is not ready: ' + pod['metadata']['name'])
    return dict(pod=pod['metadata']['name'], uid=pod['metadata']['uid'],
                node=pod['spec']['nodeName'], containers=[
                    {k: c.get(k) for k in ('name', 'imageID', 'containerID', 'restartCount')}
                    for c in pod['status']['containerStatuses']])


def placement_issues(expected, observed, brokers):
    """Original identities stay fixed. Added pod identities may change, not their nodes."""
    issues = []
    current = {p['pod']: p for p in observed}
    originals = {p['pod']: p for p in expected['clients']}
    for name, original in originals.items():
        if current.get(name) != original:
            issues.append(name + ': original identity, node, image or resources changed')
    exemplar = originals['consumer-sts-0']
    for name, pod in current.items():
        if name in originals:
            continue
        if name not in expected['added_nodes']:
            issues.append('Unexpected client ' + name)
            continue
        if pod['node'] != expected['added_nodes'][name]:
            issues.append(name + ': added consumer is on a different node')
        if any(pod[k] != exemplar[k] for k in ('role', 'image_id', 'resources')):
            issues.append(name + ': added consumer image or resources differ')
        if pod['restart_count'] != 0:
            issues.append(name + ': added consumer restarted')
    if sorted(brokers, key=lambda x: x['pod']) != expected['brokers']:
        issues.append('Broker identity, placement or container changed')
    return issues


def fixed_template(original, image):
    template = copy.deepcopy(original)
    spec = template['spec']
    if spec.get('affinity', {}).get('podAffinity'):
        raise ValueError('Review existing required pod affinity before adding placement anchors')
    spec['affinity']['podAffinity'] = dict(requiredDuringSchedulingIgnoredDuringExecution=[
        dict(labelSelector=dict(matchLabels={ANCHOR_LABEL: ANCHOR_VALUE}),
             matchLabelKeys=[INDEX], topologyKey='kubernetes.io/hostname')])
    for c in spec['containers'] + spec.get('initContainers', []):
        if c['name'] in ('consumer-container', 'bootstrap-consumer'):
            c['image'] = image.removeprefix('docker-pullable://') if hasattr(str, 'removeprefix') else image.replace('docker-pullable://', '')
    return template


def anchor_deployment(ordinal, node, namespace):
    labels = {ANCHOR_LABEL: ANCHOR_VALUE, INDEX: str(ordinal)}
    return dict(apiVersion='apps/v1', kind='Deployment',
        metadata=dict(name=f'matched-skew-placement-{ordinal}', namespace=namespace, labels=labels),
        spec=dict(replicas=1, selector=dict(matchLabels=labels),
            template=dict(metadata=dict(labels=labels), spec=dict(
                nodeSelector={'kubernetes.io/hostname': node},
                automountServiceAccountToken=False,
                containers=[dict(name='placement-reference', image='registry.k8s.io/pause:3.10',
                    resources=dict(requests={'cpu': '1m', 'memory': '2Mi', 'ephemeral-storage': '1Mi'},
                                   limits={'cpu': '1m', 'memory': '2Mi', 'ephemeral-storage': '1Mi'}),
                    securityContext=dict(allowPrivilegeEscalation=False, readOnlyRootFilesystem=True,
                                         capabilities=dict(drop=['ALL'])))],
                terminationGracePeriodSeconds=5))))


def assess_balanced(directory):
    analysis = load(ROOT / 'python-scripts/analyze_stability.py', 'matched_stability')
    m = json.loads((directory / 'manifest.json').read_text())
    rows = json.loads((directory / 'lag-summary.json').read_text())['snapshots']
    outcome = json.loads((directory / 'outcome-summary.json').read_text())
    end = m['producer_end_epoch']
    windows = [analysis.window(rows, end - 600, end - 300), analysis.window(rows, end - 300, end)]
    passed = (not outcome['validity_failures'] and outcome['incomplete_by_drain'] == 0
              and abs(outcome['admitted_messages_per_second'] / 600 - 1) <= BALANCE['input_rate_tolerance'])
    for w in windows:
        passed = passed and (w['coverage_fraction'] >= BALANCE['minimum_coverage']
            and w['time_weighted_mean_backlog'] is not None
            and w['time_weighted_mean_backlog'] <= BALANCE['maximum_mean_backlog']
            and w['covered_interval_growth_offsets_per_second'] <= BALANCE['maximum_growth'])
    return dict(status='passed' if passed else 'stopped_for_review', run_id=m['run_id'],
        thresholds=BALANCE, windows=windows, input_rate=outcome['admitted_messages_per_second'],
        unfinished=outcome['incomplete_by_drain'],
        message='Balanced reference passed.' if passed else 'Balanced reference did not pass. Stop for review without changing the input rate.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--evidence-root', type=Path, required=True)
    parser.add_argument('--backup-root', type=Path, required=True)
    parser.add_argument('--placement-plan', type=Path, required=True)
    parser.add_argument('--admission-plan', type=Path, required=True)
    parser.add_argument('--monitoring-gate', type=Path, required=True)
    args = parser.parse_args()
    document = json.loads(args.placement_plan.read_text())
    order = trial_order()
    protocol = dict(aggregate_rate=600, partitions=60, hot_fraction=.8,
        initial_hot_owner='consumer-sts-0', hot_partition_counts=[4, 12],
        comparison_trials=16, order=order, comparison_phases_seconds=[60, 600, 120],
        balanced_reference_phases_seconds=[60, 1200, 120], stress=STRESS,
        balanced_admission=BALANCE, placement=document,
        decision='Scheduled action at evaluation second 60. No adaptive controller or splitting.',
        figures=dict(categories=['Performance evaluation metrics', 'Runtime control signals', 'Resource use'],
                     cpu_axis='CPU use (cores)', memory_axis='Memory use (MiB)',
                     presentation='Two-run means, observed ranges, shared axes and short labels.'))
    if not args.execute:
        print(json.dumps(protocol, indent=2))
        return
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed code before execution')
    support = load(ROOT / 'experiments/twelve-hot600-20261104/run.py', 'matched_support')
    base = support.load_base()
    r = base.r
    if subprocess.check_output(['kubectl', 'config', 'current-context']).decode().strip() != 'nautilus' or r.NS != 'kafkastreamingdata':
        raise RuntimeError('Unexpected cluster or namespace')
    root = args.evidence_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    audit = root / ('matched-series-' + time.strftime('%Y%m%d-%H%M%S'))
    audit.mkdir()
    write_json(audit / 'protocol.json', protocol)
    state = dict(status='preparing', code_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
                 started_epoch=time.time(), stress_controls=[], calibration=None)
    def save(**values):
        state.update(values)
        write_json(audit / 'series-status.json', state)
    save()
    print('[MATCHED SERIES]', audit, flush=True)
    if shutil.which('caffeinate'):
        subprocess.Popen(['caffeinate', '-is', '-w', str(os.getpid())])
    support.use_local_evidence(base, root)
    from compress_evidence import compress
    from compress_monitoring_csv import compress_csv
    original_verify = r.verify_node_placement
    def verify(control, stage, consumers=None):
        result = original_verify(control, stage, consumers)
        items = json.loads(r.kubectl('get', 'pods', '-o', 'json'))['items']
        clients = [pod_identity(p) for p in items if p['metadata'].get('labels', {}).get('app') in ('consumer-sts', 'producer-workers')]
        brokers = [broker_identity(p) for p in items if p['metadata'].get('labels', {}).get('app.kubernetes.io/name') == 'kafka']
        issues = placement_issues(document, clients, brokers)
        r.journal(control, 'matched-placement-checks.jsonl', dict(timestamp=time.time(), stage=stage, clients=clients, brokers=brokers, issues=issues))
        if issues:
            raise RuntimeError('Matched placement failed: ' + ', '.join(issues))
        return result
    r.verify_node_placement = verify
    original_complete = r.complete_run
    def complete(plan=None):
        directory = original_complete(plan)
        r.stop()
        m = json.loads((directory / 'manifest.json').read_text())
        stress = None
        if not (plan or {}).get('prepare_only'):
            if m['config']['TRAFFIC_MODE'] == 'skew' and (plan or {}).get('action') == 'none':
                stress = check_saved_control(directory, dict(stress=STRESS))
                state['stress_controls'].append(dict(hot_count=len(m['config']['SKEW_PARTITION'].split(',')), directory=str(directory.resolve()), report=stress))
                write_json(directory / 'matched-stress-admission.json', stress)
                save()
            compress(directory)
            compress_csv(directory)
        support.preserve(directory, args.backup_root)
        if stress:
            print('[PRESSURE]', json.dumps(stress), flush=True)
            require_passed(stress)
        return directory
    r.complete_run = complete
    original_prepared = base.prepared
    def prepared(arm, number, seed, reference=None, hot=None, gate=False, aggregate_rate=600, hot_count=12, deadline_ms=1000):
        selected, response = split_arm(arm)
        config, plan = original_prepared(response, number, seed, reference,
            hot[:selected] if hot is not None else None, gate=gate, aggregate_rate=600,
            hot_count=selected, deadline_ms=1000)
        config['data']['SEPARATE_BROKER_NODES'] = 'false'
        config['data']['EXP_ID'] = f'matched-hot{selected}-{response}-run{number}' + ('-technical' if gate else '')
        return config, plan
    base.prepared = prepared
    original_protocol = base.campaign_protocol
    def campaign_protocol(*_args, **_kwargs):
        p = original_protocol(600, False, 12, 1000)
        p.update(performance_trials=16, order=order,
            conditions={f'hot{n}__{a}': p['conditions'][a] for n in (4, 12) for a in ARMS},
            matched_workloads=protocol, calibration_reference=state['calibration']['run_id'])
        p['lag_analysis_parameters']['growth_window_samples'] = 15
        p['outcomes'] = [x.replace('10-second growth', '30-second growth') for x in p['outcomes']]
        return p
    base.campaign_protocol = campaign_protocol
    def ready(campaign, arm):
        n, response = split_arm(arm)
        passed = {x['hot_count'] for x in state['stress_controls'] if x['report']['status'] == 'passed'}
        if response != 'keep3' and passed != {4, 12}:
            raise AdmissionStopped('Both Keep 3 workloads must demonstrate pressure before any mitigation.')
    base.require_comparison_ready = ready
    template_original = None
    template_installed = None
    original_strategy = None
    created = []
    try:
        with r.command_lock():
            current = r.read_control()
            if current and current.get('state') in ('running', 'preparing'):
                raise RuntimeError('Another run is active')
            items = json.loads(r.kubectl('get', 'pods', '-o', 'json'))['items']
            clients = [pod_identity(p) for p in items if p['metadata'].get('labels', {}).get('app') in ('consumer-sts', 'producer-workers')]
            brokers = [broker_identity(p) for p in items if p['metadata'].get('labels', {}).get('app.kubernetes.io/name') == 'kafka']
            issues = placement_issues(document, clients, brokers)
            if issues or len(clients) != 6:
                raise RuntimeError('Starting placement differs: ' + str(issues))
            sts = json.loads(r.kubectl('get', 'statefulset', 'consumer-sts', '-o', 'json'))
            if sts['spec']['replicas'] != 3:
                raise RuntimeError('Expected three consumer pods')
            write_json(audit / 'original-consumer-statefulset.json', sts)
            template_original = sts['spec']['template']
            original_strategy = sts['spec']['updateStrategy']
            image = next(p['image_id'] for p in document['clients'] if p['pod'] == 'consumer-sts-0')
            template_installed = fixed_template(template_original, image)
            for name, node in document['added_nodes'].items():
                ordinal = int(name.rsplit('-', 1)[1])
                deployment = anchor_deployment(ordinal, node, r.NS)
                r.kubectl('create', '-f', '-', input=json.dumps(deployment).encode())
                created.append(deployment['metadata']['name'])
                save(placement_anchors=created)
            for name in created:
                r.kubectl('rollout', 'status', 'deployment/' + name, '--timeout=180s', timeout=190, request_timeout='190s')
            patch = [dict(op='test', path='/spec/template', value=template_original),
                     dict(op='test', path='/spec/updateStrategy', value=original_strategy),
                     dict(op='replace', path='/spec/updateStrategy', value=dict(type='OnDelete')),
                     dict(op='replace', path='/spec/template', value=template_installed)]
            r.kubectl('patch', 'statefulset', 'consumer-sts', '--type=json', '-p', json.dumps(patch))
            save(placement_installed=True)
        # Calibration uses the same processing settings, resources and original nodes.
        with r.command_lock():
            original = r.shared_read(r.CONFIG)
            (audit / 'original-config.yaml').write_bytes(original)
            config, plan = prepared('kafka_scale6', 0, 71)
            config['data'].update(TRAFFIC_MODE='balanced', EXP_DURATION_SEC='1260',
                EXP_ID='matched-balanced600', WORKLOAD_SEED='71')
            plan.update(action='none', target_consumers=None)
            expected = yaml.safe_dump(config, sort_keys=False).encode()
            try:
                r.stop()
                r.shared_write(r.CONFIG, expected)
                save(status='balanced_calibration')
                with r.paused_for_experiment(r, plan), r.prometheus_connection():
                    r.preflight()
                    directory = r.complete_run(plan)
                result = assess_balanced(directory)
                write_json(audit / 'balanced-admission.json', result)
                save(calibration=dict(directory=str(directory.resolve()), run_id=result['run_id'], admission=result))
                support.preserve(audit, args.backup_root / 'calibration-stage')
                require_passed(result)
            finally:
                r.stop()
                if base.normalized(r.shared_read(r.CONFIG)) != base.normalized(expected):
                    raise RuntimeError('Shared configuration changed externally. Restoration withheld.')
                r.shared_write(r.CONFIG, original)
                if r.shared_read(r.CONFIG) != original:
                    raise RuntimeError('Configuration restoration failed')
        save(status='comparison')
        sys.argv = [sys.argv[0], '--execute', '--aggregate-rate', '600', '--hot-partitions', '12',
            '--slo-ms', '1000', '--monitoring-gate', str(args.monitoring_gate),
            '--admission-plan', str(args.admission_plan), '--audit-root', str(root)]
        campaign = base.main()
        save(status='complete', campaign_directory=str(campaign.resolve()))
        support.preserve(campaign, args.backup_root)
        print('[COMPLETE] Sixteen matched comparison trials finished.', flush=True)
    except BaseException as exc:
        save(status='stopped_for_review' if isinstance(exc, AdmissionStopped) else 'failed', error=str(exc) or type(exc).__name__)
        print('[STOPPED]', state['error'], flush=True)
        raise
    finally:
        try:
            with r.command_lock():
                current = r.read_control()
                if current and current.get('state') in ('running', 'preparing'):
                    raise RuntimeError('Active run prevents automatic placement restoration')
                if state.get('placement_installed'):
                    r.set_consumer_baseline(3, 180)
                    patch = [dict(op='test', path='/spec/template', value=template_installed),
                        dict(op='test', path='/spec/updateStrategy', value=dict(type='OnDelete')),
                        dict(op='replace', path='/spec/template', value=template_original),
                        dict(op='replace', path='/spec/updateStrategy', value=original_strategy)]
                    r.kubectl('patch', 'statefulset', 'consumer-sts', '--type=json', '-p', json.dumps(patch))
                for name in created:
                    r.kubectl('delete', 'deployment', name, '--wait=true', '--timeout=60s', timeout=70)
                save(restoration='verified', finished_epoch=time.time())
        except BaseException as exc:
            save(restoration='pending', restoration_error=str(exc), finished_epoch=time.time())
            raise
        finally:
            support.preserve(audit, args.backup_root / 'final-stage')
    return audit


if __name__ == '__main__':
    main()
