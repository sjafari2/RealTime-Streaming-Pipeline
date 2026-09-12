"""Known outcomes and boundary cases; these tests do not call Nautilus."""
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'python-scripts'), str(ROOT / 'my-shell')]
from analyze_execution import quantity, pod_observation, resource_integral, recovery, analyze
from evaluate_run import evaluate
from summarize_runs import summarize
from test_analysis import make_run, LATENCIES
import run_experiment as runner
from test_run_commands import batch_setup, evidence


def metadata(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    for role in ('producer', 'consumer'):
        path = directory / role / 'events.jsonl'
        events = [json.loads(line) for line in path.read_text().splitlines()]
        for row in events:
            index = int(row['message_id'])
            row.update(topic=manifest['run_id']+'_0', partition=index % 10, offset=index//10,
                       timestamp=row.get('completion_timestamp', 101.001), assignment_epoch=1)
        path.write_text('\n'.join(map(json.dumps, events)) + '\n')
    return directory


def test_three_quantiles_and_partition_denominators(tmp_path):
    directory = metadata(make_run(tmp_path/'run'))
    row = evaluate(directory)
    assert not row['validity_failures']
    assert [row[f'admitted_cohort_completion_p{q}_seconds'] for q in (50,95,99)] == pytest.approx([.06,.1,.15])
    parts = row['partition_metrics']['partitions']
    assert len(parts) == 10 and sum(p['admitted_messages'] for p in parts) == 20
    assert parts[0]['admitted_per_second'] == .2
    assert parts[0]['acknowledged_arrivals_per_second'] == .2
    assert parts[0]['unique_completions_per_second'] == .2
    assert parts[0]['completion_mean_seconds'] == pytest.approx(.035)
    assert row['partition_metrics']['correctness']['within_epoch_ordering_comparisons'] == 10


def test_partition_rate_population_and_unfinished_are_separate(tmp_path):
    directory = metadata(make_run(tmp_path/'run', values=[10,20]))
    path = directory/'consumer/events.jsonl'
    events = [json.loads(line) for line in path.read_text().splitlines()]
    # Admission is in evaluation; completion occurs during drain.
    events[1]['completion_timestamp'] = 111
    path.write_text('\n'.join(map(json.dumps, events)))
    row = evaluate(directory)
    p = row['partition_metrics']['partitions'][1]
    assert p['admitted_messages'] == p['completed_cohort_messages'] == 1
    assert p['unique_completions_per_second'] == 0 and p['deadline_miss_fraction'] == 1


def test_conflicting_identity_and_within_epoch_order_are_detected(tmp_path):
    directory = metadata(make_run(tmp_path/'run'))
    path = directory/'consumer/events.jsonl'
    events = [json.loads(line) for line in path.read_text().splitlines()]
    events[0]['offset'] = 5  # ACK says offset 0; following partition-0 completion is offset 1.
    path.write_text('\n'.join(map(json.dumps, events)))
    row = evaluate(directory)
    assert row['partition_metrics']['correctness']['conflicting_message_identities'] > 0
    assert row['partition_metrics']['correctness']['within_epoch_ordering_violations'] == 1
    assert row['validity_failures']


def test_partition_pooling_and_plan_assignment_grouping(tmp_path):
    a = metadata(make_run(tmp_path/'a','a',values=[10,20]))
    b = metadata(make_run(tmp_path/'b','b',values=[100]))
    for directory in (a,b):
        path = directory/'manifest.json'; m=json.loads(path.read_text())
        m['initial_assignment'] = [dict(pod='c',topic=m['run_id']+'_0',partition=0)]
        path.write_text(json.dumps(m))
    result = summarize([a,b])
    assert len(result['groups']) == 1
    p = result['groups'][0]['partitions'][0]['pooled_messages']
    assert p['completion_mean_seconds'] == pytest.approx(.055)
    assert p['completion_p50_seconds'] == pytest.approx(.01)
    assert p['completion_p95_seconds'] == pytest.approx(.1)
    path = b/'manifest.json'; m=json.loads(path.read_text()); m['intervention']={'action':'scale'}
    path.write_text(json.dumps(m))
    assert len(summarize([a,b])['groups']) == 2


@pytest.mark.parametrize('value,expected', [('500m',.5),('1000000000n',1),('2Gi',2**31),('1.5G',1.5e9),('1e3',1000),('0',0)])
def test_resource_units(value, expected):
    assert quantity(value) == expected


def sample(t, names=('c0',), valid=True):
    return dict(timestamp=t,valid=valid,pods=[dict(uid=n,pod=n,node='node',phase='Running',cpu_request_cores=.5,memory_request_gib=2) for n in names])


def test_resource_integration_retains_removed_pods_and_excludes_gaps():
    rows = [sample(0,('c0','c1')),sample(5,('c0','c1')),sample(10),sample(30)]
    result = resource_integral(rows,0,30,10)
    assert result['covered_seconds'] == 10
    assert result['covered_fraction'] == pytest.approx(1/3)
    assert result['scheduled_consumer_pod_seconds_observed'] == 20
    assert result['consumer_container_requested_cpu_seconds_observed'] == 10
    assert result['consumer_container_requested_gib_seconds_observed'] == 40
    rows[1]['valid'] = False
    assert resource_integral(rows,0,30,10)['covered_seconds'] == 0


def test_process_cost_sums_finished_lifetimes_including_removed_incarnation(tmp_path):
    directory = make_run(tmp_path/'run',values=[])
    for name,start,end in [('old',90,105),('new',105,122)]:
        target = directory/'consumer'/name; target.mkdir()
        (target/'final.json').write_text(json.dumps(dict(role='consumer',pod=name,incarnation=name)))
        (target/'events.jsonl').write_text(json.dumps(dict(event='started',timestamp=start))+'\n'+json.dumps(dict(event='finished',timestamp=end)))
    (directory/'consumer/events.jsonl').unlink()
    result = analyze(directory)
    assert result['consumer_process_seconds'] == 32
    assert result['consumer_lifetime_coverage_complete']


def test_recovery_requires_sustained_coverage_and_reports_censoring():
    rows = [dict(timestamp=t,valid=True,processing_backlog=b,owners={'p':'c'}) for t,b in [(0,50),(2,5),(4,0),(6,0),(8,0)]]
    result = recovery(rows,0,8,5,6,3)
    assert result['seconds_to_confirmation'] == 8 and not result['censored']
    rows[2]['valid'] = False
    result = recovery(rows,0,8,5,6,3)
    assert result['censored'] and result['seconds_to_confirmation'] is None
    assert recovery([],0,8,5,6,3)['status'] == 'unavailable'


def test_process_duration_uses_monotonic_evidence_across_wall_clock_adjustment(tmp_path):
    directory=make_run(tmp_path/'run',values=[])
    (directory/'consumer/events.jsonl').write_text(json.dumps(dict(event='started',timestamp=100))+'\n'+
        json.dumps(dict(event='finished',timestamp=90,lifetime_seconds=15)))
    result=analyze(directory)
    assert result['consumer_process_seconds']==15
    assert next(r for r in result['process_lifetimes'] if r['role']=='consumer')['duration_source']=='monotonic'


def test_intervention_validation_and_guard(monkeypatch, tmp_path):
    config = dict(EXP_DURATION_SEC='300',RETENTION_MS='86400000',ACKS='all')
    plan = dict(action='scale',after_evaluation_start_seconds=60,initial_consumers=2,target_consumers=3)
    runner.validate_intervention(plan,config)
    for change in [dict(target_consumers=2),dict(after_evaluation_start_seconds=300),dict(recovery_threshold_offsets=0),dict(action='reassign')]:
        with pytest.raises(ValueError): runner.validate_intervention(dict(plan,**change),config)
    monkeypatch.setattr(runner,'ROOT',tmp_path)
    control = dict(run_id='r',state='running',producer_end_epoch=200,intervention=dict(plan,at_epoch=100))
    monkeypatch.setattr(runner,'time',type('Clock',(),{'time':staticmethod(lambda:101)}))
    monkeypatch.setattr(runner,'read_control',lambda:dict(run_id='other',state='running'))
    calls=[]; monkeypatch.setattr(runner,'kubectl',lambda *a,**kw:calls.append(a))
    with pytest.raises(RuntimeError,match='Shared run changed'): runner.apply_intervention(control)
    assert not calls
    monkeypatch.setattr(runner,'read_control',lambda:control)
    runner.apply_intervention(control)
    assert calls[0] == ('scale','statefulset','consumer-sts','--current-replicas=2','--replicas=3')
    saved=[json.loads(line) for line in (tmp_path/'results/r/intervention-events.jsonl').read_text().splitlines()]
    assert saved[0]['scheduling_delay_seconds'] == 1 and saved[-1]['event'] == 'request_completed'


def test_no_action_records_decision_without_scaling(monkeypatch, tmp_path):
    monkeypatch.setattr(runner,'ROOT',tmp_path)
    control=dict(run_id='r',state='running',producer_end_epoch=200,intervention=dict(action='none',at_epoch=100))
    monkeypatch.setattr(runner,'read_control',lambda:control)
    monkeypatch.setattr(runner,'time',type('Clock',(),{'time':staticmethod(lambda:101)}))
    def forbidden(*args,**kwargs): raise AssertionError('No-action must not scale')
    monkeypatch.setattr(runner,'kubectl',forbidden)
    runner.apply_intervention(control)
    assert (tmp_path/'results/r/intervention-events.jsonl').exists()


def test_repeated_runs_pass_same_declared_plan(monkeypatch, tmp_path):
    batch_setup(monkeypatch,tmp_path)
    monkeypatch.setattr(runner,'read_config',lambda:(b'',dict(RETENTION_MS='86400000',ACKS='all')))
    plan=dict(action='scale',after_evaluation_start_seconds=60,initial_consumers=2,target_consumers=3)
    calls=[]
    def complete(actual):
        calls.append(actual)
        return evidence(tmp_path,'run-'+str(len(calls)))[0]
    monkeypatch.setattr(runner,'complete_run',complete)
    runner.run_complete_commands(repetitions=3,batch=True,plan=plan)
    assert calls == [plan,plan,plan]
    status=json.loads(next((tmp_path/'results/batches').glob('*/batch-status.json')).read_text())
    assert status['intervention']==plan and status['status']=='complete'


def test_baseline_waits_for_exact_ready_replica_count(monkeypatch):
    calls=[]
    def kubectl(*args,**kwargs):
        calls.append(args)
        if args[:2]==('get','statefulset'): return json.dumps(dict(spec=dict(replicas=3))).encode()
        if args[:2]==('get','pods'):
            return json.dumps(dict(items=[dict(metadata=dict(name=str(i)),status=dict(conditions=[dict(type='Ready',status='True')])) for i in range(2)])).encode()
        return b''
    monkeypatch.setattr(runner,'kubectl',kubectl)
    runner.set_consumer_baseline(2,5)
    assert calls[1]==('scale','statefulset','consumer-sts','--current-replicas=3','--replicas=2')


def test_stop_cancels_pending_intervention_before_drain(monkeypatch):
    control=dict(run_id='r',state='running',producer_end_epoch=200,drain_end_epoch=260,drain_seconds=60,
                 intervention=dict(action='scale',at_epoch=150))
    monkeypatch.setattr(runner,'read_control',lambda:control)
    monkeypatch.setattr(runner,'time',type('Clock',(),{'time':staticmethod(lambda:101)}))
    monkeypatch.setattr(runner,'publish',lambda c:None)
    monkeypatch.setattr(runner,'force_stop_role',lambda role:[])
    def wait(c):
        assert c['intervention_cancelled_epoch']==101 and c['producer_end_epoch']==101
    monkeypatch.setattr(runner,'wait_for_end',wait)
    runner.stop()
    assert control['state']=='stopped'


def test_interrupted_runs_are_not_pooled_even_with_complete_message_evidence(tmp_path):
    directory=metadata(make_run(tmp_path/'run'))
    (directory/'runner-status.json').write_text(json.dumps(dict(status='interrupted_or_failed')))
    group=summarize([directory])['groups'][0]
    assert group['included_run_count']==0 and group['excluded_runs']
    assert group['pooled_messages']['valid_completion_count']==0
