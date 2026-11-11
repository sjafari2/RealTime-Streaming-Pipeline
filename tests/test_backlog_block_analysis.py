import copy
import pytest

from analyze_backlog_block import validate_block, ARMS


def block():
    order = [(c + '__' + a, 1, 91) for a in ARMS for c in ('smaller', 'larger')]
    series = dict(status='complete', restoration='verified',
                  starting_backlog_contrast={'status': 'passed'},
                  starting_backlog_preparation={'status': 'passed'})
    campaign = dict(status='complete', restoration='verified',
        runs=[dict(arm=a, run_number=n, seed=s, run_id='run-' + str(i),
                   validation={'status': 'passed'}) for i, (a, n, s) in enumerate(order)],
        cost_protocol={'matched_workloads': dict(order=order,
            comparison_mode='starting-backlog', warmup_aggregate_rates={'smaller': 300, 'larger': 900},
            aggregate_rate=600, hot_partition_counts=[12], comparison_phases_seconds=[60, 600, 120])})
    return series, campaign


def test_backlog_block_requires_validated_distinct_runs_and_preparation():
    series, campaign = block()
    validate_block(series, campaign)
    for change in ('missing', 'duplicate', 'invalid', 'older_seed', 'different_rate'):
        bad = copy.deepcopy(campaign)
        if change == 'missing':
            bad['runs'].pop()
        elif change == 'duplicate':
            bad['runs'][1]['run_id'] = bad['runs'][0]['run_id']
        elif change == 'invalid':
            bad['runs'][0]['validation']['status'] = 'failed'
        elif change == 'older_seed':
            bad['runs'][0]['seed'] = 83
        else:
            bad['cost_protocol']['matched_workloads']['aggregate_rate'] = 700
        with pytest.raises(ValueError):
            validate_block(series, bad)
    for field in ('starting_backlog_contrast', 'starting_backlog_preparation'):
        bad = copy.deepcopy(series)
        bad.pop(field)
        with pytest.raises(ValueError):
            validate_block(bad, campaign)


def test_backlog_analysis_waits_for_completed_restored_execution():
    series, campaign = block()
    with pytest.raises(ValueError):
        validate_block(dict(series, status='running'), campaign)
    with pytest.raises(ValueError):
        validate_block(series, dict(campaign, restoration='pending'))
