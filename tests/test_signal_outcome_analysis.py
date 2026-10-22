"""Check analysis boundaries, missing observations and incomplete resource cost."""
import copy
import importlib.util
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1]/'experiments/signal-outcome-20261021/build_analysis.py'
if not SOURCE.exists():
    SOURCE = Path(__file__).with_name('build_analysis.py')
spec=importlib.util.spec_from_file_location('signal_outcome_analysis',SOURCE)
analysis=importlib.util.module_from_spec(spec);spec.loader.exec_module(analysis)


def snapshots():
    return [dict(timestamp=1000+t+.0002,valid=True,owners={'p':'consumer-sts-2'},
                 positions={'p':t},highs={'p':4*t},lags={'p':3*t},
                 total_lag=3*t,processing_backlog=3*t,skew=1,max_partition_lag=3*t,
                 persistent_hot=['p']) for t in range(0,62,2)]


def test_pre_action_grid_excludes_action_and_uses_30_second_growth():
    rows=snapshots();rows[-1]['total_lag']=10**9
    result=analysis.signal_features(rows,1000)
    assert result['sample_count']==15
    assert result['mean_backlog_offsets']==132
    assert result['backlog_growth_30s_offsets_per_second']==pytest.approx(3)
    assert result['total_backlog_last_sample']==174


@pytest.mark.parametrize('change',['missing','owner','offset','invalid'])
def test_discontinuous_pre_action_window_is_not_filled(change):
    rows=snapshots()
    if change=='missing':del rows[20]
    elif change=='owner':rows[20]['owners']={'p':'consumer-sts-0'}
    elif change=='offset':rows[20]['positions']={'p':-1}
    else:rows[20]['valid']=False
    result=analysis.signal_features(rows,1000)
    assert result['valid'] is False
    assert result['mean_backlog_offsets'] is None
    assert result['backlog_growth_30s_offsets_per_second'] is None


def process_series():
    result=[]
    for suffix,value in [('cpu_percent',100),('memory_bytes',2**20)]:
        labels={'pod':'consumer-sts-0','incarnation':'one'}
        result.append({'metric':dict(labels,__name__='consumer_'+suffix),
                       'values':[[1000+t,value] for t in range(30,60,2)]})
        result.append({'metric':dict(labels,__name__='consumer_'+suffix+'_scrape_timestamp_seconds'),
                       'values':[[1000+t,1000+t-1] for t in range(30,60,2)]})
    return result


def test_process_resources_require_fresh_complete_observations():
    series=process_series()
    assert analysis.resource_features(series,['consumer-sts-0'],1000)['cpu_percent']['total_mean']==1
    series[1]['values'][0][1]=0
    result=analysis.resource_features(series,['consumer-sts-0'],1000)
    assert result['cpu_percent']['total_mean'] is None
    assert result['cpu_percent']['complete_samples']==14
    assert result['memory_bytes']['total_mean']==1


def test_duplicate_process_incarnations_are_not_summed():
    series=process_series();extra=copy.deepcopy(series[:2])
    for row in extra:row['metric']['incarnation']='two'
    result=analysis.resource_features(series+extra,['consumer-sts-0'],1000)
    assert result['cpu_percent']['total_mean'] is None


def test_process_restart_inside_window_invalidates_resource_mean():
    series=process_series();extra=copy.deepcopy(series[:2])
    for row in series[:2]:row['values']=row['values'][:7]
    for row in extra:
        row['metric']['incarnation']='two'
        row['values']=row['values'][7:]
    result=analysis.resource_features(series+extra,['consumer-sts-0'],1000)
    assert result['cpu_percent']['total_mean'] is None
    assert result['memory_bytes']['total_mean']==1


def test_incomplete_request_coverage_does_not_become_full_cost():
    row={'requested_resources_evaluation_and_drain':dict(covered_fraction=.02,
        consumer_container_requested_cpu_seconds_observed=72,
        consumer_container_requested_gib_seconds_observed=36)}
    result=analysis.outcome_cost(row)
    assert result['requested_cpu_core_minutes'] is None
    assert result['request_coverage']==.02
