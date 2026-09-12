import importlib.util
import json
import math
from pathlib import Path
import sys

import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'python-scripts'))
sys.path.insert(0,str(ROOT/'my-shell'))
from evaluate_run import evaluate
from summarize_runs import summarize
from analyze_lag import signals, analyze
from run_experiment import estimate_capacity

LATENCIES=[10,20,20,30,30,40,40,50,50,60,60,70,70,80,80,90,90,100,100,150]


def make_run(root, run_id='r', values=LATENCIES, unfinished=False, duplicate=False, failed=False, rate='2'):
    root.mkdir()
    config=dict(SLO_THRESHOLD_MS='99',TARGET_RATE=rate,TOPIC_TITLE=run_id,TOPIC_COUNT='1',NUM_PARTITIONS='10',RUN_ID=run_id)
    manifest=dict(run_id=run_id,evaluation_start_epoch=100,producer_end_epoch=110,drain_end_epoch=120,
                  config=config,producer_pods=['p'],consumer_pods=['c'])
    (root/'manifest.json').write_text(json.dumps(manifest))
    for role,pod in [('producer','p'),('consumer','c')]:
        path=root/role;path.mkdir()
        (path/'final.json').write_text(json.dumps(dict(run_id=run_id,role=role,pod=pod,incarnation=pod,
                                                       failure='test failure' if failed else None)))
    admitted=[dict(event='acknowledged',message_id=str(i),producer_timestamp=101) for i in range(len(values))]
    completed=[dict(event='completed',message_id=str(i),producer_timestamp=101,completion_timestamp=101+v/1000,
                    processing_start_timestamp=101+(v-2)/1000,processing_seconds=.002,output_sha256='same')
               for i,v in enumerate(values) if not (unfinished and i==len(values)-1)]
    if duplicate: completed.append(dict(completed[0],completion_timestamp=101.5,processing_start_timestamp=101.498))
    (root/'producer/events.jsonl').write_text('\n'.join(map(json.dumps,admitted)))
    (root/'consumer/events.jsonl').write_text('\n'.join(map(json.dumps,completed)))
    return root


def test_worked_example_with_pending_and_duplicate(tmp_path):
    row=evaluate(make_run(tmp_path/'run',unfinished=True,duplicate=True))
    assert row['admitted_cohort_valid_completion_count']==19
    assert row['admitted_cohort_completion_latency_sum_seconds']==pytest.approx(1.09)
    assert row['admitted_cohort_completion_mean_seconds']==pytest.approx(1.09/19)
    assert row['admitted_cohort_completion_p99_seconds']==pytest.approx(.1)
    assert row['deadline_miss_fraction']==pytest.approx(.15)
    assert row['incomplete_fraction']==pytest.approx(.05)
    assert row['useful_throughput_per_second']==pytest.approx(1.9)
    assert row['completed_attempts_per_second']==pytest.approx(2)
    assert row['duplicate_completion_attempts']==1
    assert row['diagnostic_cohort_latencies']['processing_seconds']['mean']==pytest.approx(.002)
    assert row['diagnostic_cohort_latencies']['start_seconds']['mean']==pytest.approx(1.09/19-.002)


def test_no_completions_is_not_zero_latency(tmp_path):
    row=evaluate(make_run(tmp_path/'run',values=[10],unfinished=True))
    assert row['admitted_cohort_completion_mean_seconds'] is None
    assert row['admitted_cohort_completion_p99_seconds'] is None
    assert row['deadline_miss_fraction']==1


def test_repetitions_pool_messages_not_quantiles_and_exclude_invalid(tmp_path):
    a=make_run(tmp_path/'a','a',values=[10,20])
    b=make_run(tmp_path/'b','b',values=[100])
    bad=make_run(tmp_path/'bad','bad',values=[10000],failed=True)
    other=make_run(tmp_path/'other','other',values=[999],rate='3')
    result=summarize([a,b,bad,other])
    assert len(result['groups'])==2
    group=next(g for g in result['groups'] if g['settings']['config']['TARGET_RATE']=='2')
    assert group['included_run_count']==2 and len(group['excluded_runs'])==1
    assert group['pooled_messages']['completion_mean_seconds']==pytest.approx(.13/3)
    assert group['pooled_messages']['completion_p99_seconds']==pytest.approx(.1)
    assert group['run_level']['admitted_cohort_completion_p99_seconds']['mean']==pytest.approx(.06)
    assert group['run_level']['admitted_cohort_completion_mean_seconds']['mean']==pytest.approx(.0575)
    with pytest.raises(ValueError,match='Duplicate RUN_ID'):summarize([a,a])


def snapshot(t, lags, owner='c'):
    return dict(timestamp=t,valid=True,lags={str(i):v for i,v in enumerate(lags)},
                owners={str(i):owner for i in range(len(lags))},
                positions={str(i):0 for i in range(len(lags))},highs={str(i):v for i,v in enumerate(lags)})


def test_lag_formulas_and_coverage():
    rows=[snapshot(t,[v]*8+[9*v]*2) for t,v in [(0,10),(2,20),(4,30)]]
    result=signals(rows,window_samples=2,hot_k=1,minimum_lag=10,persistence=1)
    last=result['snapshots'][-1]
    assert last['total_lag']==780
    assert last['skew']==pytest.approx(270/78)
    assert last['population_stddev']==96
    assert last['growth_offsets_per_second']==130
    assert last['window_growth_offsets_per_second']==130
    assert last['persistent_hot']==['8','9']
    assert result['backlog_area_offset_seconds']==2080
    assert result['time_weighted_mean_lag']==520
    assert result['covered_seconds']==4
    # k=2 sits exactly on the high-group boundary; the proposal uses strict >.
    assert signals([snapshot(0,[0]*8+[100]*2)],hot_k=2)['snapshots'][0]['hot']==[]


def test_missing_and_changed_owner_break_growth():
    rows=[snapshot(0,[10,10]),dict(timestamp=2,valid=False,invalid_reasons=['missing']),snapshot(4,[20,20]),snapshot(6,[30,30],'new')]
    result=signals(rows,window_samples=2)
    assert result['covered_seconds']==0
    assert result['time_weighted_mean_lag'] is None
    assert result['snapshots'][-1]['growth_offsets_per_second'] is None
    assert result['snapshots'][-1]['persistent_hot'] is None


def test_balanced_capacity_uses_actual_replicas():
    row=estimate_capacity({'TARGET_RATE':'3000','APP_DELAY_MS':'0'},3,6)
    assert row['target_messages_per_second']==9000
    assert row['balanced_messages_per_second_per_consumer']==1500
    assert row['balanced_processing_budget_ms']==pytest.approx(2/3)


def test_lag_export_rejects_stale_and_duplicate_owners(tmp_path):
    root=make_run(tmp_path/'run',values=[10])
    data=[]
    for partition in range(10):
        metrics={'consumer_partition_owned':1,'consumer_lag_valid':1,
                 'consumer_lag':5,'consumer_position_offset':10,'consumer_high_offset':15,
                 'consumer_processing_backlog':0}
        for name,value in metrics.items():
            data.append(dict(metric=dict(__name__=name,run_id='r',topic='r_0',partition=str(partition),pod='c',incarnation='c'),
                             values=[[100,str(value)],[102,str(value)],[110,str(value)]]))
        data.append(dict(metric=dict(__name__='consumer_lag_observed_timestamp_seconds',run_id='r',topic='r_0',partition=str(partition),pod='c',incarnation='c'),
                         values=[[100,'100'],[102,'102'],[110,'90']]))
    path=root/'prometheus.json'
    path.write_text(json.dumps(dict(status='success',data=dict(result=data))))
    result=analyze(root)
    assert result['covered_seconds']==2
    assert result['covered_fraction']==pytest.approx(.2)
    assert result['final_lag'] is None
    assert result['parameters']['window_samples']==15
    assert not result['snapshots'][-1]['valid']
    duplicate=dict(metric=dict(data[0]['metric'],pod='other'),values=[[102,'1']])
    data.append(duplicate)
    path.write_text(json.dumps(dict(status='success',data=dict(result=data))))
    result=analyze(root)
    assert result['covered_seconds']==0
    assert 'Duplicate owner for r_0/0' in result['snapshots'][1]['invalid_reasons']
