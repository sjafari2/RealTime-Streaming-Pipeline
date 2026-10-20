"""Check that result summaries neither bridge handovers nor turn unknown into zero."""
import importlib.util
from pathlib import Path
import pytest
import sys

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'experiments/result-metric-audit-20260930/build_report.py'
if not SOURCE.exists(): SOURCE=Path(__file__).with_name('build_report.py')
spec=importlib.util.spec_from_file_location('result_metric_audit',SOURCE)
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
sys.path.insert(0,str(SOURCE.parent))
from build_compact_figures import runtime_states, process_total


def point(t,value,offset=0,owner='C0',valid=True):
    return dict(timestamp=t,valid=valid,owners={'P0':owner},positions={'P0':offset},
                highs={'P0':100},total_lag=value,persistent_hot_count=value)


def test_sample_mean_is_distinct_from_time_weighted_diagnostic():
    rows=[point(0,0),point(2,2),point(5,8)]
    result=audit.stats(rows,'total_lag',5)
    assert result['time_weighted_mean']==pytest.approx(3.4)
    assert result['sample_mean']==pytest.approx(10/3)
    assert result['covered_seconds']==5
    assert result['sampled_peak']==8


def test_missing_and_owner_changes_are_not_bridged():
    rows=[point(0,10),point(2,20),point(4,90,valid=False),point(6,1,owner='C1'),point(8,3,owner='C1')]
    result=audit.stats(rows,'total_lag',8)
    assert result['covered_seconds']==4
    assert result['time_weighted_mean']==8.5
    assert result['sample_mean']==8.5
    assert result['defined_samples']==4
    assert result['sampled_peak']==20


def test_offset_decrease_and_long_gap_break_intervals():
    assert not audit.continuous(point(0,10,offset=5),point(2,0,offset=4))
    assert not audit.continuous(point(0,10),point(4,0))
    assert not audit.continuous(point(0,10),point(2,0,owner='C1'))


def test_unknown_detector_is_not_zero():
    rows=[point(0,None),point(2,0),point(4,0)]
    result=audit.stats(rows,'persistent_hot_count',4)
    assert result['time_weighted_mean']==0
    assert result['sample_mean']==0
    assert audit.stats([point(0, None)], 'total_lag', 2)['sample_mean'] is None
    assert result['covered_seconds']==2
    assert result['defined_samples']==2
    series=audit.compact_series([point(0,1),point(2,2,owner='C1')],0)
    assert len(series)==3
    assert series[1]['persistent_hot_count'] is None


def test_lag_minima_use_valid_partition_samples():
    rows = [dict(point(0, 14), lags={'P0': 3, 'P1': 11}),
            dict(point(2, 0, valid=False), lags={'P0': 0, 'P1': 0}),
            dict(point(4, 10), lags={'P0': 4, 'P1': 6})]
    assert audit.lag_minima(rows) == {'min_total_lag': 10, 'min_partition_lag': 3}


def test_lag_minima_preserve_missing_and_real_zero():
    assert audit.lag_minima([point(0, 0, valid=False)]) == {
        'min_total_lag': None, 'min_partition_lag': None}
    rows = [dict(point(0, 10), lags={'P0': 0, 'P1': 10})]
    assert audit.lag_minima(rows) == {'min_total_lag': 10, 'min_partition_lag': 0}


@pytest.mark.parametrize('rounding', [-.0003, 0, .0003])
def test_pre_mitigation_mean_uses_same_fifteen_samples(rounding):
    rows = [point(1000 + t + rounding, t) for t in range(0, 64, 2)]
    # The boundary at 60 must be excluded despite export timestamp rounding.
    result = audit.pre_mitigation_backlog(rows, 1000)
    assert result['valid_window']
    assert result['sample_count'] == 15
    assert result['mean_total_backlog'] == 44
    assert result['observed_first_sample_seconds'] == pytest.approx(30 + rounding)
    assert result['observed_last_sample_seconds'] == pytest.approx(58 + rounding)


@pytest.mark.parametrize('failure', ['missing', 'invalid', 'owner', 'offset', 'duplicate'])
def test_pre_mitigation_mean_does_not_substitute_or_bridge_samples(failure):
    rows = [point(t, t, offset=t) for t in range(0, 64, 2)]
    index = next(i for i, row in enumerate(rows) if row['timestamp'] == 44)
    if failure == 'missing':
        rows.pop(index)
    elif failure == 'invalid':
        rows[index]['valid'] = False
    elif failure == 'owner':
        rows[index]['owners'] = {'P0': 'C1'}
    elif failure == 'offset':
        rows[index]['positions'] = {'P0': 0}
    else:
        rows.insert(index, dict(rows[index]))
    result = audit.pre_mitigation_backlog(rows, 0)
    assert not result['valid_window']
    assert result['mean_total_backlog'] is None


def test_ending_mean_excludes_recovery_and_drain():
    rows = [point(t, 20000 if t < 570 else 50) for t in range(0, 602, 2)]
    rows[-2]['total_lag'] = 80
    rows[-1]['total_lag'] = 99999
    result = audit.end_evaluation_backlog(rows, 0, 600)
    assert result['valid_window']
    assert result['mean_total_backlog'] == 52
    assert result['total_backlog_at_last_sample'] == 80
    assert result['sample_count'] == 15
    assert result['nominal_first_sample_seconds'] == 570
    assert result['nominal_last_sample_seconds'] == 598
    rows = [row for row in rows if row['timestamp'] != 598]
    assert audit.end_evaluation_backlog(rows, 0, 600)['mean_total_backlog'] is None
    assert audit.end_evaluation_backlog(rows, 0, 600)['total_backlog_at_last_sample'] is None


def test_runtime_preserves_full_set_and_original_window_growth():
    rows=[dict(point(0,9),window_mean_backlog=8,window_growth_offsets_per_second=3,
               window_mean_skew=2,persistent_hot=['topic/2','topic/5']),
          dict(point(2,0,owner='C1'),persistent_hot=None),
          dict(point(4,0,owner='C1'),window_mean_backlog=0,window_growth_offsets_per_second=0,
               window_mean_skew=0,persistent_hot=[])]
    states=runtime_states(rows,0,4)
    assert states[0]['H_persistent']==['topic/2','topic/5']
    assert states[0]['G_W']==3
    assert states[0]['state_available']
    assert states[1]['break_before'] and states[1]['H_persistent'] is None
    assert not states[1]['state_available']
    assert states[2]['H_persistent']==[] and states[2]['state_available']


def test_process_sum_does_not_treat_missing_active_process_as_zero():
    prom=[]
    for pod,values,stamps in [('C0',[[0,100],[2,100],[4,100]],[[0,0],[2,2],[4,4]]),
                             ('C1',[[4,50]],[[4,4]])]:
        prom.extend([dict(metric={'pod':pod,'__name__':'cpu'},values=values),
                     dict(metric={'pod':pod,'__name__':'cpu_scrape_timestamp_seconds'},values=stamps)])
    output=process_total(prom,'cpu',100,{'C0':-1,'C1':1},0,4)
    assert output['values']==[1,None,1.5]


def test_pre_action_skew_averages_ratios_on_the_declared_grid():
    rows = [dict(point(t, 1), skew=1, max_partition_lag=1, mean_partition_lag=1)
            for t in range(0, 64, 2)]
    for row in rows:
        if row['timestamp'] == 30:
            row.update(skew=10, max_partition_lag=100, mean_partition_lag=10)
        if row['timestamp'] == 60:
            row['skew'] = 999  # The action boundary is outside the window.
    result = audit.pre_mitigation_skew(rows, 0)
    assert result['valid_window'] and result['sample_count'] == 15
    assert result['mean_skew'] == pytest.approx(1.6)
    assert result['mean_skew'] != pytest.approx(114 / 24)  # Ratio of sums is different.


@pytest.mark.parametrize('failure', ['missing', 'undefined', 'nonfinite', 'owner', 'reset'])
def test_pre_action_skew_rejects_incomplete_or_discontinuous_windows(failure):
    rows = [dict(point(t, 20, offset=t), skew=5) for t in range(30, 60, 2)]
    if failure == 'missing':
        rows.pop(7)
    elif failure == 'undefined':
        rows[7]['skew'] = None
    elif failure == 'nonfinite':
        rows[7]['skew'] = float('nan')
    elif failure == 'owner':
        rows[7]['owners'] = {'P0': 'C1'}
    else:
        rows[7]['positions'] = {'P0': 0}
    result = audit.pre_mitigation_skew(rows, 0)
    assert not result['valid_window']
    assert result['mean_skew'] is None
