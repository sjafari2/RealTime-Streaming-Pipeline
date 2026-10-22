"""Keep task time, effective capacity and missing calibration evidence distinct."""
import importlib.util
from pathlib import Path
import sys

import pytest

folder=Path(__file__).resolve().parents[1]/'experiments/signal-outcome-20261021'
if not folder.exists():folder=Path(__file__).parent
sys.path.insert(0,str(folder))
spec=importlib.util.spec_from_file_location('capacity_analysis',folder/'build_capacity.py')
capacity=importlib.util.module_from_spec(spec);spec.loader.exec_module(capacity)


def series(name,pod,values,**labels):
    return dict(metric=dict(__name__=name,pod=pod,run_id='test',incarnation='a',**labels),
                values=[[1000+t,v] for t,v in values])


def producer_data():
    ticks=list(range(20,62,2));rows=[]
    for name,rate,labels in [('producer_delivery_ok_total',150,{}),
        ('producer_partition_messages_total',100,dict(topic='test_0',partition='0')),
        ('producer_partition_messages_total',50,dict(topic='test_0',partition='1'))]:
        rows.append(series(name,'p0',[(t,t*rate) for t in ticks],**labels))
    rows.append(series('producer_cpu_percent_scrape_timestamp_seconds','p0',[(t,1000+t) for t in ticks]))
    manifest=dict(run_id='test',evaluation_start_epoch=1000,producer_pods=['p0'],
                  config=dict(TOPIC_TITLE='test',TOPIC_COUNT='1',NUM_PARTITIONS='2'))
    return dict(status='success',data=dict(result=rows)),manifest


def test_partition_rates_reconcile_with_acknowledged_total():
    export,manifest=producer_data();data=capacity.CounterData(export,manifest)
    result=capacity.partition_arrivals(data,manifest)
    assert result['partitions']=={'0/0':100,'0/1':50}
    assert result['aggregate_rate']==150


@pytest.mark.parametrize('fault',['reset','missing','stale','duplicate','mismatch'])
def test_bad_counter_windows_are_unavailable(fault):
    export,manifest=producer_data();rows=export['data']['result']
    if fault=='reset':rows[1]['values'][8][1]=0
    elif fault=='missing':del rows[1]['values'][4]
    elif fault=='stale':rows[-1]['values'][8][1]=0
    elif fault=='duplicate':rows.append(rows[1])
    elif fault=='mismatch':rows[0]['values'][-2][1]+=10
    with pytest.raises(ValueError):
        capacity.partition_arrivals(capacity.CounterData(export,manifest),manifest)


def test_effective_service_rate_includes_time_outside_processing_timer():
    ticks=list(range(50,122,2));rows=[]
    for name,rate in [('consumer_messages_consumed_total',50),
        ('consumer_processing_seconds_count',50),('consumer_processing_seconds_sum',.5)]:
        rows.append(series(name,'c0',[(t,t*rate) for t in ticks]))
    rows.append(series('consumer_cpu_percent_scrape_timestamp_seconds','c0',[(t,1000+t) for t in ticks]))
    rows.append(series('consumer_rebalances_total','c0',[(t,1) for t in range(130,142,2)],event_type='revoke'))
    for name,value in [('consumer_processing_backlog',1000),('consumer_partition_owned',1)]:
        rows.append(series(name,'c0',[(t,value) for t in ticks],topic='test_0',partition='0'))
    manifest=dict(run_id='test',evaluation_start_epoch=1000)
    lag=dict(snapshots=[dict(timestamp=1000+t,valid=True,owners={'test_0/0':'c0/a'},
        positions={'test_0/0':t*50},highs={'test_0/0':t*50+1000}) for t in ticks])
    result=capacity.service_window(capacity.CounterData(dict(status='success',data=dict(result=rows)),manifest),lag,manifest,'c0',60,120)
    assert result['eligible']
    assert result['mean_processing_seconds']==.01
    assert result['task_only_rate']==100
    assert result['effective_rate']==50
    assert result['minimum_processing_backlog']==1000


def test_cumulative_arrival_counter_can_bridge_missing_interior_query():
    export,manifest=producer_data()
    for row in export['data']['result']:del row['values'][8]
    result=capacity.partition_arrivals(capacity.CounterData(export,manifest),manifest)
    assert result['aggregate_rate']==150
    assert result['producer_intervals'][0]['missing_interior_query_samples']==[36]


def test_assignment_with_unmeasured_consumer_is_not_approved():
    result=capacity.assignment_check({'0/0':100},{'0/0':'c0'},[],{})
    assert not result['complete_capacity_coverage']
    assert not result['all_below_observed_minimum']
    assert result['consumers'][0]['effective_rate'] is None


def test_aggregate_partition_load_is_compared_to_consumer_capacity():
    window=dict(eligible=True,stable_rate=True,completions=6000,observed_seconds=60,
                effective_rate=100,mean_processing_seconds=.005,processing_count=6000,processing_seconds=30)
    refs=[dict(run_id='cal',condition={'node':'one'},windows=[window])]
    result=capacity.assignment_check({'0/0':60,'0/1':50},{'0/0':'c0','0/1':'c0'},refs,{'c0':{'node':'one'}})
    assert result['consumers'][0]['arrival_rate']==110
    assert result['consumers'][0]['reference_status']=='above_observed_mean_capacity'
    assert result['consumers'][0]['load_ratio']==1.1


def test_underloaded_throughput_is_not_reported_as_capacity():
    row=capacity.summarize_windows([dict(eligible=False,stable_rate=True,effective_rate=80,
        completions=4800,observed_seconds=60,processing_count=4800,processing_seconds=24)])
    assert row['effective_rate'] is None
    assert row['highest_demonstrated_rate']==80


def test_capacity_is_not_transferred_to_a_different_node():
    window=dict(eligible=True,stable_rate=True,completions=6000,observed_seconds=60,
                effective_rate=100,processing_count=6000,processing_seconds=30)
    refs=[dict(run_id='cal',condition={'node':'one'},windows=[window])]
    result=capacity.assignment_check({'0/0':50},{'0/0':'c0'},refs,{'c0':{'node':'two'}})
    assert result['consumers'][0]['effective_rate'] is None
    assert not result['consumers'][0]['conditionally_supported']
