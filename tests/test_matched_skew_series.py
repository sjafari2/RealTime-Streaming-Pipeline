"""Checks for the matched-workload design and deployment admission rules."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys

ROOT = Path(os.environ['PIPELINE_REPOSITORY']) if 'PIPELINE_REPOSITORY' in os.environ else Path(__file__).resolve().parents[1]
SCRIPT = Path(os.environ.get('MATCHED_SERIES_SCRIPT', ROOT / 'experiments/matched-skew600-20261104/run.py'))
spec = importlib.util.spec_from_file_location('matched_series', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_balanced_order_has_two_baselines_before_mitigations():
    order = m.trial_order()
    assert len(order) == len(set(order)) == 16
    assert order[:2] == [('hot12__keep3', 1, 81), ('hot4__keep3', 1, 81)]
    assert [a for a, n, s in order[8:]] == list(reversed([a for a, n, s in order[:8]]))
    assert {(m.split_arm(a)[0], m.split_arm(a)[1], n) for a, n, s in order} == {
        (h, a, n) for h in (4, 12) for a in m.ARMS for n in (1, 2)}


def test_per_consumer_placement_rejects_permuted_added_nodes_and_broker_restart():
    original = dict(pod='consumer-sts-0', node='old', uid='original', role='consumer', image_id='image', resources={'cpu': 2}, restart_count=0)
    broker = dict(pod='broker-0', uid='b', node='broker-node', containers=[{'restartCount': 0}])
    expected = dict(clients=[original], added_nodes={'consumer-sts-3': 'A', 'consumer-sts-4': 'B'}, brokers=[broker])
    added = [dict(original, pod='consumer-sts-3', node='A', uid='new3'), dict(original, pod='consumer-sts-4', node='B', uid='new4')]
    assert m.placement_issues(expected, [original] + added, [broker]) == []
    added[0]['node'], added[1]['node'] = 'B', 'A'
    assert len(m.placement_issues(expected, [original] + added, [broker])) == 2
    bad_broker = copy.deepcopy(broker)
    bad_broker['containers'][0]['restartCount'] = 1
    assert any('Broker' in x for x in m.placement_issues(expected, [original], [bad_broker]))


def test_fixed_template_preserves_separation_and_pins_image_and_ordinal():
    original = {'spec': {'affinity': {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [{'original': True}]}},
                         'containers': [{'name': 'consumer-container', 'image': 'latest', 'resources': {'requests': {'cpu': '2'}}}],
                         'initContainers': [{'name': 'bootstrap-consumer', 'image': 'latest'}]}}
    before = copy.deepcopy(original)
    actual = m.fixed_template(original, 'repo@sha256:123')
    assert original == before
    assert actual['spec']['affinity']['podAntiAffinity'] == before['spec']['affinity']['podAntiAffinity']
    term = actual['spec']['affinity']['podAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]
    assert term['matchLabelKeys'] == ['apps.kubernetes.io/pod-index']
    assert actual['spec']['containers'][0]['resources'] == before['spec']['containers'][0]['resources']
    assert actual['spec']['initContainers'][0]['image'] == 'repo@sha256:123'


def test_balanced_admission_rejects_underproduction_and_missing_work(tmp_path):
    start = 10000
    manifest = dict(evaluation_start_epoch=start, producer_end_epoch=start + 1200, run_id='fixture')
    rows = [dict(timestamp=start + t, valid=True, processing_backlog=20, owners={'0': 'c0'}, highs={'0': t + 20}, positions={'0': t}) for t in range(0, 1201, 2)]
    outcome = dict(validity_failures=[], incomplete_by_drain=0, admitted_messages_per_second=600)
    for name, value in [('manifest.json', manifest), ('lag-summary.json', {'snapshots': rows}), ('outcome-summary.json', outcome)]:
        (tmp_path / name).write_text(json.dumps(value))
    assert m.assess_balanced(tmp_path)['status'] == 'passed'
    outcome['admitted_messages_per_second'] = 580
    (tmp_path / 'outcome-summary.json').write_text(json.dumps(outcome))
    assert m.assess_balanced(tmp_path)['status'] == 'stopped_for_review'
    outcome['admitted_messages_per_second'] = 600
    outcome['incomplete_by_drain'] = 1
    (tmp_path / 'outcome-summary.json').write_text(json.dumps(outcome))
    assert m.assess_balanced(tmp_path)['status'] == 'stopped_for_review'


def test_stress_requires_each_late_window_not_only_full_run_average():
    from comparison_admission import stress_check
    manifest = dict(run_id='fixture', evaluation_start_epoch=0, producer_end_epoch=600, intervention={'action': 'none'})
    rows = [dict(timestamp=t, valid=True, total_lag=20000 + 150 * t, owners={'p': 'c0'}) for t in range(0, 601, 2)]
    assert stress_check(manifest, rows, m.STRESS)['status'] == 'passed'
    for row in rows:
        if row['timestamp'] >= 540:
            row['total_lag'] = 101000
    assert stress_check(manifest, rows, m.STRESS)['status'] == 'stopped_for_review'
