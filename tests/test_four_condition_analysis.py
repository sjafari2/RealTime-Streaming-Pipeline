import importlib.util
import os
from pathlib import Path
import numpy as np
import pytest
from copy import deepcopy
ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
spec=importlib.util.spec_from_file_location('four_cost',Path(os.environ.get('CAPACITY_SUMMARY_SCRIPT',ROOT/'experiments/four-condition-20260929/summarize.py')))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def test_parallel_processing_prevents_a_false_global_pause():
    result=m.longest_processing_gap(np.array([0.,1.,3.]),np.array([2.,4.,5.]),0,5)
    assert result['seconds']==0


def test_processing_gap_uses_union_and_clips_observation_boundaries():
    result=m.longest_processing_gap(np.array([-1.,.2,4.,7.]),np.array([1.,2.,5.,8.]),0,6)
    assert result==dict(start_epoch=2.,end_epoch=4.,seconds=2.)


def test_native_settlement_restarts_on_missing_data_or_ownership_change():
    owners={str(i):'consumer-sts-'+str(i%6)+'/process' for i in range(60)}
    rows=[dict(timestamp=t,valid=True,owners=dict(owners)) for t in range(0,30,2)]
    rows[2]['valid']=False
    rows[5]['owners']['0']='consumer-sts-1/process'
    result=m.stable_six(rows,0)
    assert result['first_epoch']==12 and result['confirmed_epoch']==22


def test_five_owners_do_not_satisfy_native_scale_settlement():
    owners={str(i):'consumer-sts-'+str(i%5)+'/process' for i in range(60)}
    assert m.stable_six([dict(timestamp=t,valid=True,owners=owners) for t in range(20)],0) is None


def test_growth_plot_uses_ten_seconds_without_changing_saved_evidence():
    rows=[dict(timestamp=t,valid=True,lags={'p':t*t},processing_backlog=t*t,
               owners={'p':'c'},positions={'p':0},highs={'p':t*t}) for t in range(0,32,2)]
    lag=dict(parameters=dict(window_samples=15),snapshots=m.signals(rows)['snapshots'])
    original=deepcopy(lag)
    result=m.growth_plot_lag(lag)
    assert result['snapshots'][5]['window_processing_backlog_growth_offsets_per_second']==10
    assert result['snapshots'][4]['window_processing_backlog_growth_offsets_per_second'] is None
    assert result['parameters']['growth_window_samples']==5
    assert lag==original
    for saved,updated in zip(lag['snapshots'],result['snapshots']):
        assert {k:v for k,v in saved.items() if k not in ('window_growth_offsets_per_second','window_processing_backlog_growth_offsets_per_second')}=={k:v for k,v in updated.items() if k not in ('window_growth_offsets_per_second','window_processing_backlog_growth_offsets_per_second')}
    lag['snapshots'][2]['timestamp']=4.5
    with pytest.raises(ValueError,match='2-second export grid'):m.growth_plot_lag(lag)


def test_two_condition_campaign_requires_all_planned_trials():
    order=[['redistribute3',1,81],['scale_redistribute6',1,81],
           ['scale_redistribute6',2,82],['redistribute3',2,82]]
    campaign=dict(status='complete',restoration='verified',
        cost_protocol=dict(order=order,performance_trials=4,aggregate_input_rate=700,
                           conditions={'redistribute3':'three','scale_redistribute6':'six'}),
        runs=[dict(arm=a,run_number=n,seed=s,validation={'status':'passed'}) for a,n,s in order])
    assert m.campaign_design(campaign)==(['redistribute3','scale_redistribute6'],700)
    campaign['runs'].pop()
    with pytest.raises(ValueError,match='planned verified'):m.campaign_design(campaign)


def test_unreviewed_rate_is_rejected_before_analysis():
    order=[['redistribute3',1,81],['redistribute3',2,82]]
    campaign=dict(status='complete',restoration='verified',
        cost_protocol=dict(order=order,performance_trials=2,aggregate_input_rate=900,
                           conditions={'redistribute3':'three'}),
        runs=[dict(arm=a,run_number=n,seed=s,validation={'status':'passed'}) for a,n,s in order])
    with pytest.raises(ValueError,match='Unreviewed aggregate'):m.campaign_design(campaign)


def test_metric_availability_does_not_count_undefined_as_zero():
    lag=dict(snapshots=[dict(timestamp=0,valid=True,mean_partition_lag=5,persistent_hot=None),
                        dict(timestamp=2,valid=True,mean_partition_lag=3,persistent_hot=[]),
                        dict(timestamp=4,valid=False),
                        dict(timestamp=6,valid=True,mean_partition_lag=4,persistent_hot=['p'])])
    counts=m.lag_metric_coverage(lag)
    assert counts['mean_partition_lag']['defined_samples']==3
    assert counts['persistent_hot']['defined_samples']==2
    lines=m.diagnostic_lines(lag,0,['persistent_hot'])
    finite=[v for v in lines[0][1] if np.isfinite(v)]
    assert finite==[0,1]
