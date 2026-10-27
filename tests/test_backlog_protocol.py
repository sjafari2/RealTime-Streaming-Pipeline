"""The two preparation histories must not change the evaluation workload."""
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('backlog_campaign', ROOT / 'experiments/backlog-comparison/run.py')
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def test_two_runs_of_each_response_at_each_backlog_level(monkeypatch):
    document = dict(aggregate_input_rate=600, hot_partition_count=12,
        warmup_aggregate_rates={'smaller': 300, 'larger': 900},
        initial_hot_owner='consumer-sts-0', calibration_reference='test-calibration',
        technical_evidence=['test-only'])
    campaign.install(document)
    monkeypatch.setattr(campaign.base.r, 'validate_intervention', lambda *args: None)
    order = campaign.trial_order()
    assert len(order) == 16
    assert [a for a, _, _ in order[:8]] == list(reversed([a for a, _, _ in order[8:]]))
    assert len({a for a, _, _ in order}) == 8
    initial = {'assignment': [{'partition': p, 'pod': 'consumer-sts-' + str(p//20)} for p in range(60)]}
    settings = []
    for arm, number, seed in order:
        config, plan = campaign.base.prepared(arm, number, seed, initial, list(range(12)))
        data = config['data']
        steps = json.loads(data['PRODUCER_RATE_SCHEDULE_JSON'])
        assert steps[-1] == {'at_seconds': 60, 'rate': 200}
        assert steps[0]['rate'] == (100 if arm.startswith('smaller') else 300)
        settings.append(tuple(data[k] for k in ('TARGET_RATE', 'NUM_PARTITIONS', 'SKEW_PARTITION',
                         'SKEW_FRACTION', 'APP_CPU_ITERATIONS', 'WARMUP_SECONDS', 'EXP_DURATION_SEC', 'DRAIN_SECONDS')))
        assert plan['after_evaluation_start_seconds'] == 60
        if arm.endswith('kafka_scale6'):
            assert plan['action'] == 'scale' and 'target_assignment' not in plan
            assert 'EXPLICIT_ASSIGNMENT_JSON' not in data
    assert len(set(settings)) == 1
    protocol = campaign.base.campaign_protocol()
    assert protocol['performance_trials'] == 16
    assert protocol['lag_analysis_parameters']['growth_window_samples'] == 15
    assert len(protocol['conditions']) == 8
