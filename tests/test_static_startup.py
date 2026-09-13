"""Check that startup validation cannot silently become a traffic experiment."""
import copy
import json
import time
from types import SimpleNamespace

import pytest

from test_measurements import consumer, FakeConsumer, FakeRuntime
from test_placement_control import example
from placement_control import PlacementMismatch, capture_reference, check_placement, reference_hash
import run_experiment as r
import static_startup as s


def test_static_ids_are_unique_per_pod_even_with_one_shared_configuration():
    config = {'CONSUMER_STATIC_MEMBERSHIP': 'true'}
    ids = [consumer.membership_settings(f'consumer-sts-{n}', config)['group.instance.id'] for n in range(6)]
    assert len(set(ids)) == 6
    assert ids[0] == consumer.membership_settings('consumer-sts-0', config)['group.instance.id']
    assert consumer.membership_settings('consumer-sts-0', {}) == {}
    with pytest.raises(ValueError): consumer.membership_settings('consumer-sts-0', {'CONSUMER_STATIC_MEMBERSHIP':'ture'})


def test_static_setting_reaches_client_and_readiness_evidence(monkeypatch):
    monkeypatch.setenv('CONSUMER_STATIC_MEMBERSHIP', 'true')
    monkeypatch.setenv('CONSUMER_GROUP_ID', 'test-static')
    monkeypatch.setenv('BOOTSTRAP_SERVERS', 'unused:9092')
    monkeypatch.setenv('TOPIC_TITLE', 'test')
    monkeypatch.setenv('PARTITION_ASSIGNMENT_STRATEGY', 'cooperative-sticky')
    monkeypatch.setattr(consumer, 'Runtime', FakeRuntime)
    monkeypatch.setattr(consumer, 'Consumer', FakeConsumer)
    worker = consumer.MetricConsumer()
    assert worker.consumer.config['group.instance.id'] == worker.runtime.pod
    assert worker.runtime.details['group_instance_id'] == worker.runtime.pod
    assert worker.runtime.phase == 'ready'


def test_capture_accepts_valid_actual_ownership_then_freezes_it():
    ref, cfg, rows, pods = example()
    rows['consumer-sts-0']['assignments'], rows['consumer-sts-1']['assignments'] = rows['consumer-sts-1']['assignments'], rows['consumer-sts-0']['assignments']
    actual = capture_reference(ref['pods'], cfg, 'run', 'cfg', rows, pods)
    assert actual['assignment'][0]['pod'] == 'consumer-sts-1'
    rows['consumer-sts-0']['assignments'], rows['consumer-sts-1']['assignments'] = rows['consumer-sts-1']['assignments'], rows['consumer-sts-0']['assignments']
    with pytest.raises(PlacementMismatch, match='ownership differs'):
        check_placement(actual, cfg, 'run', 'cfg', rows, pods)


@pytest.mark.parametrize('problem', ['missing_partition', 'duplicate_partition', 'moved_pod', 'wrong_static_id'])
def test_capture_cannot_accept_incomplete_or_changed_start(problem):
    ref, cfg, rows, pods = example()
    if problem == 'missing_partition': rows['consumer-sts-0']['assignments'].pop()
    if problem == 'duplicate_partition': rows['consumer-sts-1']['assignments'].append(['fresh-run_0', 0])
    if problem == 'moved_pod': pods[1]['spec']['nodeName'] = 'changed'
    if problem == 'wrong_static_id': cfg['CONSUMER_STATIC_MEMBERSHIP'] = 'true'
    with pytest.raises((ValueError, PlacementMismatch)):
        capture_reference(ref['pods'], cfg, 'run', 'cfg', rows, pods)


@pytest.mark.parametrize('invalid', ['production', 'replace_reference'])
def test_capture_plan_cannot_release_traffic_or_replace_reference(invalid):
    ref, cfg, rows, pods = example()
    plan = dict(action='none', after_evaluation_start_seconds=60, initial_consumers=2,
                capture_placement_pods=ref['pods'], prepare_only=invalid != 'production')
    if invalid == 'replace_reference': plan['placement_reference'] = ref
    with pytest.raises(ValueError, match='capture is preparation-only'):
        r.validate_intervention(plan, cfg)


def test_capture_is_refused_at_an_intervention(monkeypatch):
    ref, cfg, rows, pods = example()
    control = dict(run_id='run', config=cfg, config_sha256='cfg', capture_placement_pods=ref['pods'])
    monkeypatch.setattr(r, 'journal', lambda *a: None)
    monkeypatch.setattr(r, 'status', lambda *a: pytest.fail('capture must stop before observation'))
    with pytest.raises(PlacementMismatch, match='cannot release production'):
        r.verify_placement(control, 'before_intervention')


def test_previous_static_members_are_waited_out(monkeypatch):
    observations = []
    rows = iter([dict(state='STABLE', members=3), dict(state='EMPTY', members=0)])
    def remote(role, pod, code, timeout):
        compile(code, '<group-clearance>', 'exec')
        assert 'KafkaError.GROUP_ID_NOT_FOUND' in code
        return json.dumps(next(rows))
    runner = SimpleNamespace(pods=lambda role: ['consumer-sts-0'], remote=remote)
    monkeypatch.setattr(s.time, 'sleep', lambda *a: None)
    result = s.wait_for_empty_group(runner, {'CONSUMER_GROUP_ID':'test-group','BOOTSTRAP_SERVERS':'kafka:9092'}, observations.append)
    assert result['members'] == 0 and len(observations) == 2


def test_uncleared_membership_cannot_start_a_stage(monkeypatch):
    clock = iter([0, 0, 0, 58, 60])
    monkeypatch.setattr(s.time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(s.time, 'sleep', lambda *a: None)
    runner = SimpleNamespace(pods=lambda role: ['consumer-sts-0'],
                             remote=lambda *a, **k: json.dumps(dict(state='STABLE', members=3)))
    with pytest.raises(PlacementMismatch, match='membership did not clear'):
        s.wait_for_empty_group(runner, {'CONSUMER_GROUP_ID':'g','BOOTSTRAP_SERVERS':'k'}, lambda row: None)


def test_default_sequence_is_empty_preparations_and_live_pair_requires_explicit_option():
    assert [row[1] for row in s.stages()] == [3, 3, 6, 3]
    assert all(row[2] for row in s.stages())
    all_stages = s.stages(True)
    assert len(all_stages) == 6
    assert [(row[0], row[4]) for row in all_stages if not row[2]] == [('scale_trial','scale'), ('keep_trial','none')]


@pytest.mark.parametrize('problem', [None, 'missing', 'traffic', 'enqueued', 'failure'])
def test_preparation_evidence_must_be_complete_and_empty(tmp_path, problem):
    manifest = dict(run_id='run', config_sha256='cfg', preparation_only=True,
                    producer_pods=['producer-sts-0'], consumer_pods=['consumer-sts-0'])
    (tmp_path/'manifest.json').write_text(json.dumps(manifest))
    for role in ('producer', 'consumer'):
        if problem == 'missing' and role == 'consumer': continue
        directory = tmp_path/role/(role+'-sts-0')/'incarnation'
        directory.mkdir(parents=True)
        final = dict(role=role, pod=role+'-sts-0', run_id='run', config_sha256='cfg', phase='finished')
        if problem == 'failure': final['failure'] = 'writer failed'
        (directory/'final.json').write_text(json.dumps(final))
        events = [dict(event='delivery_summary', enqueued=int(problem=='enqueued'), resolved=0, unresolved=0)] if role == 'producer' else []
        if problem == 'traffic': events.append(dict(event='completed'))
        (directory/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
    if problem:
        with pytest.raises(RuntimeError, match='Preparation evidence failed'): s.audit_preparation(tmp_path)
    else:
        assert s.audit_preparation(tmp_path)['valid']


@pytest.mark.parametrize('fail_at', [None, 2, 4, 5])
def test_sequence_freezes_one_reference_and_stops_after_any_failure(monkeypatch, tmp_path, fail_at):
    from placement_control import pod_identity
    _, cfg, _, template = example()
    cfg.update(PRODUCER_POD_COUNT='3', CONSUMER_POD_COUNT='3', NUM_PARTITIONS='6',
               CONSUMER_STATIC_MEMBERSHIP='true', CONSUMER_GROUP_ID='test-static', BOOTSTRAP_SERVERS='unused')
    config = {'data':cfg}
    def items(count):
        result = []
        for role, maximum in [('producer',3),('consumer',count)]:
            for ordinal in range(maximum):
                item = copy.deepcopy(template[0 if role == 'producer' else 1])
                name = f'{role}-sts-{ordinal}'
                item['metadata'].update(name=name,uid=name+'-uid')
                item['spec']['nodeName'] = name+'-node'
                result.append(item)
        return result
    original = [pod_identity(item) for item in items(3)]
    block = dict(attempts=[], runs=[], preparation_seconds_used=0, preparation_verified=False)
    class Runner:
        def __init__(self):
            self.count=3
            self.plans=[]
            self.current={}
        def stop(self): pass
        def set_consumer_baseline(self,count,timeout): self.count=count
        def kubectl(self,*args): return json.dumps({'items':items(self.count)})
        def read_control(self): return self.current
        def complete_run(self,plan):
            self.plans.append(copy.deepcopy(plan))
            if len(self.plans) == fail_at:
                raise PlacementMismatch('simulated startup or action failure')
            directory=tmp_path/str(len(self.plans))
            directory.mkdir()
            c=copy.deepcopy(cfg)
            c['CONSUMER_POD_COUNT']=str(self.count)
            if plan.get('capture_placement_pods'):
                rows={}
                for p in plan['capture_placement_pods']:
                    ordinal=int(p['pod'].rsplit('-',1)[1])
                    rows[p['pod']]=dict(role=p['role'],pod=p['pod'],run_id='run',config_sha256='cfg',phase='ready',
                        incarnation=p['pod'],assignment_epoch=1,group_instance_id=p['pod'],
                        assignments=[[c['TOPIC_TITLE']+'_0',n] for n in range(6) if (n+1)%self.count==ordinal] if p['role']=='consumer' else [])
                ref=capture_reference(plan['capture_placement_pods'],c,'run','cfg',rows,items(self.count))
            else:
                ref=plan['placement_reference']
            self.current=dict(run_id='run'+str(len(self.plans)),placement_reference=ref,
                placement_reference_sha256=reference_hash(ref),preparation_only=plan['prepare_only'],
                preparation_elapsed_seconds=.1,preparing_epoch=time.time())
            if not plan['prepare_only']: self.current['start_epoch']=time.time()
            (directory/'manifest.json').write_text(json.dumps(self.current))
            return directory
    runner=Runner()
    monkeypatch.setattr(s,'wait_for_empty_group',lambda *a,**k: {'state':'EMPTY','members':0})
    monkeypatch.setattr(s,'audit_preparation',lambda *a: {'valid':True})
    def write(path,value): path.write_text(json.dumps(value))
    def execute():
        s.execute_stages(runner,tmp_path,block,config,original,lambda:None,write,
                         lambda cfg:None,lambda:None,include_comparison=True)
    if fail_at:
        with pytest.raises(PlacementMismatch): execute()
        assert len(runner.plans)==fail_at
        assert block['attempts'][-1]['status']=='failed'
    else:
        execute()
        assert block['status']=='complete' and block['preparation_verified']
        assert [row['action'] for row in block['runs']]==['scale','none']
        fixed=[p['placement_reference'] for p in runner.plans if 'placement_reference' in p]
        assert len(fixed)==4 and len({reference_hash(ref) for ref in fixed})==1
        assert fixed[0]['assignment'][0]['pod']=='consumer-sts-1'
    assert all(plan['prepare_only'] for plan in runner.plans[:4])
