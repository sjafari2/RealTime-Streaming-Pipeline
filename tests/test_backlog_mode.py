"""Check that backlog preparation cannot bypass workload or pressure checks."""
import copy
import importlib.util
import json
import os
from pathlib import Path

import pytest

SCRIPT = Path(os.environ['MATCHED_SERIES_SCRIPT'])
spec = importlib.util.spec_from_file_location('backlog_mode', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def controls():
    return [dict(hot_count=12, condition=level, report={'status': 'passed'},
                 decision_backlog={'total_backlog': count})
            for level, count in [('smaller', 15000), ('larger', 45000)]]


def test_new_deployment_is_an_eight_trial_block_with_both_controls_first():
    order = m.trial_order(1, 1, 'starting-backlog')
    assert len(order) == len(set(order)) == 8
    assert order[:2] == [('smaller__keep3', 1, 91), ('larger__keep3', 1, 91)]
    assert {a for a, _, _ in order} == {c + '__' + a for c in ('smaller', 'larger') for a in m.ARMS}
    assert all(m.split_arm(a)[0] == 12 for a, _, _ in order)


def test_schedule_changes_only_warmup_and_preserves_common_continuing_rate():
    for level, warmup in [('smaller', 100), ('larger', 300)]:
        config = {'data': {'TARGET_RATE': '200', 'WARMUP_SECONDS': '60', 'EXP_DURATION_SEC': '660'}}
        m.set_backlog_schedule(config, level)
        assert json.loads(config['data']['PRODUCER_RATE_SCHEDULE_JSON']) == [
            {'at_seconds': 0, 'rate': warmup}, {'at_seconds': 60, 'rate': 200}]
        assert config['data']['TARGET_RATE'] == '200'
    with pytest.raises(ValueError):
        m.set_backlog_schedule({'data': {'WARMUP_SECONDS': '20'}}, 'smaller')


def test_contrast_requires_measured_larger_backlog_and_both_admitted_controls():
    items = controls()
    assert m.assess_backlog_contrast(items)['status'] == 'passed'
    items[1]['decision_backlog']['total_backlog'] = 10000
    assert m.assess_backlog_contrast(items)['status'] == 'stopped_for_review'
    items[1]['report']['status'] = 'stopped_for_review'
    with pytest.raises(m.AdmissionStopped):
        m.assess_backlog_contrast(items)


def test_resume_cannot_count_two_twelve_partition_controls_as_one_condition():
    protocol = dict(comparison_mode='starting-backlog', order=m.trial_order(1, 1, 'starting-backlog'))
    series = dict(status='failed', restoration='verified', calibration={'admission': {'status': 'passed'}},
                  stress_controls=controls())
    campaign = dict(status='failed', restoration='verified',
                    runs=[{'arm': a} for a, _, _ in protocol['order'][:3]],
                    cost_protocol={'matched_workloads': json.loads(json.dumps(protocol))})
    m.validate_resume_source(series, campaign, protocol)
    missing = copy.deepcopy(series)
    missing['stress_controls'].pop()
    with pytest.raises(ValueError):
        m.validate_resume_source(missing, campaign, protocol)
    changed = copy.deepcopy(series)
    changed['stress_controls'][1]['decision_backlog']['total_backlog'] = 10000
    with pytest.raises(ValueError):
        m.validate_resume_source(changed, campaign, protocol)


def test_decision_backlog_does_not_use_future_or_stale_observations(tmp_path):
    (tmp_path / 'manifest.json').write_text(json.dumps({'run_id': 'fixture', 'evaluation_start_epoch': 100}))
    rows = [{'timestamp': 158, 'valid': True, 'total_lag': 2000},
            {'timestamp': 162, 'valid': True, 'total_lag': 9999}]
    (tmp_path / 'lag-summary.json').write_text(json.dumps({'snapshots': rows}))
    assert m.backlog_at_decision(tmp_path)['total_backlog'] == 2000
    rows[0]['valid'] = False
    (tmp_path / 'lag-summary.json').write_text(json.dumps({'snapshots': rows}))
    with pytest.raises(RuntimeError, match='monitoring'):
        m.backlog_at_decision(tmp_path)
