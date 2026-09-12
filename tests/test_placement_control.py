"""A controlled comparison must fail before production or scaling if its start changed."""
import copy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as runner
from placement_control import PlacementMismatch, check_placement, pod_identity, reference_hash, validate_reference


def example():
    config = dict(TOPIC_TITLE='fresh-run', TOPIC_COUNT='1', NUM_PARTITIONS='4',
                  PRODUCER_POD_COUNT='1', CONSUMER_POD_COUNT='2', ACKS='all',
                  RETENTION_MS='86400000', EXP_DURATION_SEC='300', DRAIN_SECONDS='120',
                  WARMUP_SECONDS='60', READINESS_TIMEOUT_SECONDS='180')
    items, rows = [], {}
    for role, ordinal in [('producer', 0), ('consumer', 0), ('consumer', 1)]:
        name = f'{role}-sts-{ordinal}'
        items.append(dict(metadata=dict(name=name, uid=name+'-uid', labels=dict(app=role+'-sts')),
                          spec=dict(nodeName=name+'-node', containers=[dict(name=role+'-container',
                              resources=dict(requests=dict(cpu='2', memory='1Gi'), limits=dict(cpu='4')))]),
                          status=dict(phase='Running', conditions=[dict(type='Ready', status='True')],
                              containerStatuses=[dict(name=role+'-container', ready=True, state=dict(running={}),
                                  containerID=name+'-container', imageID='sha256:known', restartCount=0)])))
        rows[name] = dict(pod=name, role=role, run_id='run', config_sha256='cfg', phase='ready',
                          incarnation=name+'-process', assignment_epoch=1,
                          assignments=[['fresh-run_0', p] for p in range(ordinal, 4, 2)] if role == 'consumer' else [])
    reference = dict(schema_version=1, pods=[pod_identity(i) for i in items],
                     assignment=[dict(topic_index=0, partition=p, pod=f'consumer-sts-{p%2}') for p in range(4)])
    return reference, config, rows, items


def test_reference_covers_all_partitions_and_accepts_generated_topic_names():
    ref, cfg, rows, pods = example()
    assert len(check_placement(ref, cfg, 'run', 'cfg', rows, pods, preparing=True)) == 3
    cfg['TOPIC_TITLE'] = 'another-run'
    for row in rows.values():
        for pair in row['assignments']:
            pair[0] = 'another-run_0'
    check_placement(ref, cfg, 'run', 'cfg', rows, pods, preparing=True)
    ref['assignment'].pop()
    with pytest.raises(ValueError, match='every configured partition'):
        validate_reference(ref, cfg)


def test_same_partition_counts_with_different_owners_are_rejected():
    ref, cfg, rows, pods = example()
    a, b = rows['consumer-sts-0'], rows['consumer-sts-1']
    a['assignments'], b['assignments'] = b['assignments'], a['assignments']
    with pytest.raises(PlacementMismatch, match='ownership differs'):
        check_placement(ref, cfg, 'run', 'cfg', rows, pods)


@pytest.mark.parametrize('field', ['uid', 'node', 'container_id', 'image_id', 'resources', 'restart_count'])
def test_same_names_do_not_hide_replacement_or_configuration_changes(field):
    ref, cfg, rows, pods = example()
    if field == 'uid': pods[1]['metadata']['uid'] = 'new-pod'
    elif field == 'node': pods[1]['spec']['nodeName'] = 'other-node'
    elif field == 'resources': pods[1]['spec']['containers'][0]['resources']['requests']['cpu'] = '3'
    else:
        key = {'container_id': 'containerID', 'image_id': 'imageID', 'restart_count': 'restartCount'}[field]
        pods[1]['status']['containerStatuses'][0][key] = 1 if field == 'restart_count' else 'new-value'
    with pytest.raises(PlacementMismatch, match='changed'):
        check_placement(ref, cfg, 'run', 'cfg', rows, pods)


@pytest.mark.parametrize('change', ['missing', 'extra', 'terminating', 'not_ready', 'old_run', 'bad_config', 'failed'])
def test_missing_stale_or_unhealthy_observations_cannot_pass(change):
    ref, cfg, rows, pods = example()
    if change == 'missing': rows.pop('consumer-sts-0')
    elif change == 'extra': pods.append(copy.deepcopy(pods[1]))
    elif change == 'terminating': pods[1]['metadata']['deletionTimestamp'] = 'now'
    elif change == 'not_ready': pods[1]['status']['containerStatuses'][0]['ready'] = False
    elif change == 'old_run': rows['consumer-sts-0']['run_id'] = 'old'
    elif change == 'bad_config': rows['consumer-sts-0']['config_sha256'] = 'changed'
    elif change == 'failed': rows['consumer-sts-0']['failure'] = 'evidence loss'
    with pytest.raises(PlacementMismatch):
        check_placement(ref, cfg, 'run', 'cfg', rows, pods)


def test_returned_original_map_does_not_hide_an_intervening_rebalance():
    ref, cfg, rows, pods = example()
    processes = check_placement(ref, cfg, 'run', 'cfg', rows, pods)
    rows['consumer-sts-0']['assignment_epoch'] += 2
    with pytest.raises(PlacementMismatch, match='epoch changed'):
        check_placement(ref, cfg, 'run', 'cfg', rows, pods, expected_processes=processes)


def test_local_observation_budget_and_failed_gate_are_journaled(monkeypatch):
    ref, cfg, rows, pods = example()
    control = dict(run_id='run', config=cfg, config_sha256='cfg', placement_reference=ref,
                   placement_reference_sha256=reference_hash(ref))
    monkeypatch.setattr(runner, 'status', lambda role, name: rows[name])
    monkeypatch.setattr(runner, 'kubectl', lambda *a, **k: json.dumps(dict(items=pods)).encode())
    clock = iter([0, 31])
    monkeypatch.setattr(runner.time, 'monotonic', lambda: next(clock))
    records = []
    monkeypatch.setattr(runner, 'journal', lambda c, file, row: records.append(copy.deepcopy(row)))
    with pytest.raises(PlacementMismatch, match='30-second'):
        runner.verify_placement(control, 'before_production')
    assert len(records) == 1 and records[0]['valid'] is False
    assert 'error' in records[0]
    assert 'initial_placement_processes' not in control


def test_failed_pre_action_gate_never_scales(monkeypatch):
    control = dict(run_id='run', state='running', producer_end_epoch=10**12,
                   intervention=dict(action='scale', at_epoch=0, initial_consumers=2, target_consumers=3))
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    def reject(*args): raise PlacementMismatch('changed owner')
    monkeypatch.setattr(runner, 'verify_placement', reject)
    monkeypatch.setattr(runner, 'kubectl', lambda *a, **k: pytest.fail('must not scale'))
    with pytest.raises(PlacementMismatch): runner.apply_intervention(control)


def test_prepare_only_collects_without_waiting_or_analyzing(monkeypatch, tmp_path):
    control = dict(run_id='prepared', state='stopped', preparation_only=True)
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'start', lambda plan: control)
    monkeypatch.setattr(runner, 'stop', lambda: None)
    monkeypatch.setattr(runner, 'collect', lambda: tmp_path)
    monkeypatch.setattr(runner, 'wait_for_end', lambda c: pytest.fail('no production clock'))
    monkeypatch.setattr(runner, 'analyze_collected', lambda *a: pytest.fail('not an experiment'))
    assert runner.complete_run(dict(prepare_only=True)) == tmp_path
    result = json.loads((tmp_path/'runner-status.json').read_text())
    assert result['status'] == 'preparation_verified' and result['workload_released'] is False


def test_prepare_only_cannot_be_a_batch():
    with pytest.raises(ValueError, match='not an experiment batch'):
        runner.run_complete_commands(repetitions=2, plan=dict(prepare_only=True))
