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


def test_mean_weights_elapsed_time():
    rows=[point(0,0),point(2,2),point(5,8)]
    result=audit.stats(rows,'total_lag',5)
    assert result['time_weighted_mean']==pytest.approx(3.4)
    assert result['covered_seconds']==5
    assert result['sampled_peak']==8


def test_missing_and_owner_changes_are_not_bridged():
    rows=[point(0,10),point(2,20),point(4,90,valid=False),point(6,1,owner='C1'),point(8,3,owner='C1')]
    result=audit.stats(rows,'total_lag',8)
    assert result['covered_seconds']==4
    assert result['time_weighted_mean']==8.5
    assert result['sampled_peak']==20


def test_offset_decrease_and_long_gap_break_intervals():
    assert not audit.continuous(point(0,10,offset=5),point(2,0,offset=4))
    assert not audit.continuous(point(0,10),point(4,0))
    assert not audit.continuous(point(0,10),point(2,0,owner='C1'))


def test_unknown_detector_is_not_zero():
    rows=[point(0,None),point(2,0),point(4,0)]
    result=audit.stats(rows,'persistent_hot_count',4)
    assert result['time_weighted_mean']==0
    assert result['covered_seconds']==2
    assert result['defined_samples']==2
    series=audit.compact_series([point(0,1),point(2,2,owner='C1')],0)
    assert len(series)==3
    assert series[1]['persistent_hot_count'] is None


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
