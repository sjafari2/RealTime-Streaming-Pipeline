"""Placement checks reject shared nodes, stale deployments and incomplete scale-up."""
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'my-shell'))
import node_placement as placement
import run_experiment as runner
from placement_control import PlacementMismatch
from test_placement_control import example


def deployment():
    sets = [yaml.safe_load((ROOT/'k8s/statefullsets'/f'{role}-sts.yaml').read_text())
            for role in ('producer', 'consumer')]
    _, config, _, items = example()
    for item in items:
        role = item['metadata']['labels']['app'].removesuffix('-sts')
        template = next(s for s in sets if s['metadata']['name'] == role+'-sts')['spec']['template']
        item['metadata']['namespace'] = runner.NS
        item['spec']['affinity'] = copy.deepcopy(template['spec']['affinity'])
    expected = {p['metadata']['name']: p['metadata']['labels']['app'].removesuffix('-sts') for p in items}
    return items, sets, expected, config


def test_authoritative_manifests_and_added_replicas_share_required_rule():
    items, sets, expected, config = deployment()
    assert not placement.template_issues(sets, runner.NS, placement.policy(config))
    assert placement.observe(items, expected, runner.NS, placement.policy(config))['valid']
    for item in sets:
        spec = item['spec']['template']['spec']
        assert placement.has_required_term(spec, runner.NS)
        assert spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']


@pytest.mark.parametrize('fault', ['shared_consumer', 'producer_shared', 'missing', 'extra', 'pending',
    'terminating', 'wrong_role', 'foreign_namespace', 'soft_rule', 'wrong_selector', 'wrong_namespace',
    'no_node', 'no_uid', 'restarted', 'different_node'])
def test_placement_failures_are_not_accepted(fault):
    items, sets, expected, config = deployment()
    rules = placement.policy(config)
    initial = placement.observe(items, expected, runner.NS, rules)['pods']
    p = items[-1]
    if fault == 'shared_consumer': p['spec']['nodeName'] = items[1]['spec']['nodeName']
    elif fault == 'producer_shared': p['spec']['nodeName'] = items[0]['spec']['nodeName']
    elif fault == 'missing': items.pop()
    elif fault == 'extra':
        p = copy.deepcopy(p);p['metadata']['name'] = 'consumer-sts-99';items.append(p)
    elif fault == 'pending':
        p['status'] = dict(phase='Pending', conditions=[dict(type='PodScheduled', status='False',
            reason='Unschedulable', message='Insufficient eligible nodes')])
    elif fault == 'terminating': p['metadata']['deletionTimestamp'] = 'now'
    elif fault == 'wrong_role': p['metadata']['labels']['app'] = 'cons-sts'
    elif fault == 'foreign_namespace': p['metadata']['namespace'] = 'elsewhere'
    elif fault == 'soft_rule': p['spec']['affinity']['podAntiAffinity'].pop('requiredDuringSchedulingIgnoredDuringExecution')
    elif fault == 'wrong_selector':
        p['spec']['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]['labelSelector']['matchExpressions'][0]['values'] = ['cons-sts']
    elif fault == 'wrong_namespace':
        p['spec']['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]['namespaces'] = ['elsewhere']
    elif fault == 'no_node': p['spec'].pop('nodeName')
    elif fault == 'no_uid': p['metadata'].pop('uid')
    elif fault == 'restarted': p['status']['containerStatuses'][0]['restartCount'] += 1
    elif fault == 'different_node': p['spec']['nodeName'] = 'replacement-node'
    result = placement.observe(items, expected, runner.NS, rules, initial)
    assert not result['valid'] and result['issues']
    if fault == 'pending': assert 'Insufficient eligible nodes' in '; '.join(result['issues'])


def test_broker_separation_is_explicit_and_other_workloads_are_reported():
    items, sets, expected, config = deployment()
    items.append(dict(metadata=dict(name='kafka-0', namespace=runner.NS, labels=placement.BROKER_LABEL),
                      spec=dict(nodeName=items[0]['spec']['nodeName']), status=dict(phase='Running')))
    report = placement.observe(items, expected, runner.NS, placement.policy(config))
    assert report['valid'] and report['namespace_colocations'][0]['kafka_label_matches']
    rules = placement.policy({'SEPARATE_BROKER_NODES': 'true'})
    for item in items[:-1]:
        item['spec']['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'].append(placement.separation_term(True))
    assert not placement.observe(items, expected, runner.NS, rules)['valid']
    items[-1]['spec']['nodeName'] = 'broker-node'
    assert placement.observe(items, expected, runner.NS, rules)['valid']
    assert not placement.observe(items, expected, runner.NS, placement.policy(config))['valid']  # Policy/template mismatch.
    for item in items[:-1]:
        item['spec']['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'].pop()
    items[-1]['metadata']['labels'] = {'app': 'unrelated'}
    assert not placement.observe(items, expected, runner.NS, rules)['valid']  # No observed broker coverage.
    items[-1]['spec']['nodeName'] = items[0]['spec']['nodeName']
    report = placement.observe(items, expected, runner.NS, placement.policy(config))
    assert report['valid'] and not report['namespace_colocations'][0]['kafka_label_matches']


def test_optional_broker_rendering_preserves_resources_volumes_and_locality():
    for role in ('consumer', 'producer'):
        path = ROOT/'k8s/statefullsets'/f'{role}-sts.yaml'
        original = yaml.safe_load(path.read_text())
        rendered = placement.render(path, separate_brokers=True)
        assert placement.has_required_term(rendered['spec']['template']['spec'], runner.NS, True)
        before, after = copy.deepcopy(original), copy.deepcopy(rendered)
        for item in (before, after):
            item['spec']['template']['spec']['affinity'].pop('podAntiAffinity')
        assert before == after
    with pytest.raises(ValueError): placement.policy({'SEPARATE_BROKER_NODES': 'maybe'})


def api_fixture(monkeypatch):
    items, sets, expected, config = deployment()
    control = dict(run_id='run', config=config, producer_pods=['producer-sts-0'],
                   consumer_pods=['consumer-sts-0','consumer-sts-1'])
    records = []
    monkeypatch.setattr(runner, 'kubectl', lambda *a, **k: json.dumps(dict(items=sets if a[1]=='statefulset' else items)).encode())
    monkeypatch.setattr(runner, 'journal', lambda c, f, row: records.append(copy.deepcopy(row)))
    return control, items, sets, records


def test_checks_capture_baseline_then_reject_changed_original_and_save_failure(monkeypatch):
    control, items, sets, records = api_fixture(monkeypatch)
    runner.verify_node_placement(control, 'before_production')
    assert control['initial_node_placement'] and control['node_placement_policy']
    items[0]['spec']['nodeName'] = 'different-node'
    with pytest.raises(PlacementMismatch, match='original pod/node'):
        runner.verify_node_placement(control, 'before_intervention')
    assert records[-1]['valid'] is False and records[-1]['error']


def test_existing_separation_does_not_excuse_a_soft_template_for_future_replicas(monkeypatch):
    control, items, sets, records = api_fixture(monkeypatch)
    sets[1]['spec']['template']['spec']['affinity']['podAntiAffinity'].pop('requiredDuringSchedulingIgnoredDuringExecution')
    with pytest.raises(PlacementMismatch, match='template lacks'):
        runner.verify_node_placement(control, 'before_production')
    assert 'initial_node_placement' not in control


def test_read_failure_is_preserved_as_unavailable_not_success(monkeypatch):
    control, items, sets, records = api_fixture(monkeypatch)
    monkeypatch.setattr(runner, 'kubectl', Mock(side_effect=RuntimeError('API unavailable')))
    with pytest.raises(RuntimeError, match='API unavailable'):
        runner.verify_node_placement(control, 'before_production')
    assert records[-1]['valid'] is False


def native_fixture(monkeypatch):
    control = dict(run_id='r', state='running', config_sha256='hash', producer_end_epoch=10**12,
        consumer_pods=['consumer-sts-0'], config=dict(READINESS_TIMEOUT_SECONDS=10, TOPIC_TITLE='t',
        TOPIC_COUNT=1, NUM_PARTITIONS=2), intervention=dict(target_consumers=2),
        initial_consumer_incarnations={'consumer-sts-0':'i0'})
    rows = [dict(run_id='r', config_sha256='hash', pod=f'consumer-sts-{i}', incarnation=f'i{i}',
        phase='running', timestamp=runner.time.time(), assignment_epoch=1, assignments=[['t_0',i]]) for i in range(2)]
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'pods', lambda role: [r['pod'] for r in rows])
    monkeypatch.setattr(runner, 'status', lambda role, name: next(r for r in rows if r['pod']==name))
    monkeypatch.setattr(runner, 'resource_snapshot', Mock())
    monkeypatch.setattr(runner, 'publish', Mock())
    monkeypatch.setattr(runner, 'journal', Mock())
    monkeypatch.setattr(runner.time, 'sleep', lambda _: None)
    monkeypatch.setattr(runner, 'verify_node_placement', Mock())
    return control, rows


def test_native_scale_requires_application_readiness_then_checks_new_nodes(monkeypatch):
    control, rows = native_fixture(monkeypatch)
    runner.wait_for_scaled_placement(control)
    runner.verify_node_placement.assert_called_once_with(control, 'after_scaling', consumers=[r['pod'] for r in rows])


def test_native_scale_rejects_sharing_and_marks_the_run_failed(monkeypatch):
    control, rows = native_fixture(monkeypatch)
    runner.verify_node_placement.side_effect = PlacementMismatch('share node')
    with pytest.raises(PlacementMismatch, match='share node'):
        runner.wait_for_scaled_placement(control)
    assert control['state'] == 'failed'


def test_native_timeout_does_not_accept_missing_or_unassigned_consumers(monkeypatch):
    control, rows = native_fixture(monkeypatch)
    rows[1]['assignments'] = []
    clock = iter([0,1,11])
    monkeypatch.setattr(runner.time, 'monotonic', lambda: next(clock))
    with pytest.raises(RuntimeError, match='readiness timed out'):
        runner.wait_for_scaled_placement(control)
    assert control['state'] == 'failed'
    assert runner.verify_node_placement.call_args.args[1] == 'scale_timeout'


def test_pre_action_node_failure_prevents_scaling(monkeypatch):
    control = dict(run_id='r', state='running', intervention={})
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'verify_placement', Mock())
    monkeypatch.setattr(runner, 'verify_node_placement', Mock(side_effect=PlacementMismatch('share node')))
    monkeypatch.setattr(runner, 'kubectl', Mock())
    with pytest.raises(PlacementMismatch): runner.apply_intervention(control)
    runner.kubectl.assert_not_called()


def test_stale_observation_cannot_capture_a_baseline(monkeypatch):
    control, items, sets, records = api_fixture(monkeypatch)
    import time
    clock = iter([0,31])
    monkeypatch.setattr(time, 'monotonic', lambda: next(clock))
    with pytest.raises(PlacementMismatch, match='freshness budget'):
        runner.verify_node_placement(control, 'before_production')
    assert 'initial_node_placement' not in control
    assert records[-1]['valid'] is False
