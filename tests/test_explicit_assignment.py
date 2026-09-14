"""Exercise the actual runtime adapter and coordinator without a live broker."""
import copy
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from confluent_kafka import TopicPartition
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src/common'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'my-shell'))
from explicit_assignment import ExplicitAssignment
from explicit_control import transfer
from pipeline_runtime import EvidenceWriter


@pytest.fixture
def worker(monkeypatch):
    monkeypatch.setenv('NUM_PARTITIONS', '2')
    monkeypatch.setenv('CONSUMER_STATIC_MEMBERSHIP', 'false')
    monkeypatch.setenv('EXPLICIT_ASSIGNMENT_JSON', json.dumps([dict(partition=0, owner='c0'), dict(partition=1, owner='c1')]))
    state = dict(state='preparing', consumer_pods=['c0', 'c1'])
    rt = SimpleNamespace(control_path='control', control=lambda: state, details={}, pod='c0',
                         incarnation='i0', run_id='r', writer=Mock(), event=Mock())
    w = SimpleNamespace(runtime=rt, topics=['t'], topic_partition=TopicPartition, assignments={}, frontiers={}, consumer=Mock())
    w.consumer.commit.side_effect = lambda offsets, asynchronous: offsets
    w.consumer.committed.side_effect = lambda offsets, timeout: [TopicPartition('t', p.partition, 17 if p.partition == 0 else 23) for p in offsets]
    def assign(consumer, parts):
        w.assignments = {(p.topic, p.partition): p for p in parts}
        w.frontiers = {(p.topic, p.partition): 17 if p.partition == 0 else 23 for p in parts}
    w.on_assign = assign
    w.forget = lambda parts: w.assignments.clear()
    w.ownership_event = Mock()
    w.adapter = ExplicitAssignment(w)
    w.adapter.initialize()
    state['explicit_handoff'] = dict(run_id='r', epoch=1, incarnations={'c0':'i0', 'c1':'i1'},
        deadline_epoch=time.time()+60, ownership=[dict(partition=0,owner='c1'),dict(partition=1,owner='c0')], stage='release')
    w.command = state['explicit_handoff']
    return w


def test_transfer_releases_before_acquiring_and_resuming(worker):
    w = worker
    assert not w.adapter.check()
    assert not w.assignments
    w.consumer.unassign.assert_called_once()
    w.runtime.writer.flush.assert_called_once()
    assert w.runtime.details['explicit_assignment']['offsets'] == {'0':17}
    w.command.update(stage='acquire', offsets={'0':17,'1':23})
    assert not w.adapter.check()
    assert set(w.assignments) == {('t',1)}
    w.consumer.resume.assert_not_called()
    w.command['stage']='resume'
    assert w.adapter.check()
    assert w.adapter.check()
    w.consumer.resume.assert_called_once()


@pytest.mark.parametrize('failure', ['commit', 'readback', 'evidence'])
def test_failed_release_never_unassigns_or_acknowledges(worker, failure):
    if failure == 'commit': worker.consumer.commit.return_value = None; worker.consumer.commit.side_effect = None
    if failure == 'readback': worker.consumer.committed.side_effect = lambda offsets, timeout: [TopicPartition('t',0,18)]
    if failure == 'evidence': worker.runtime.writer.flush.side_effect=RuntimeError('disk')
    with pytest.raises(RuntimeError): worker.adapter.check()
    worker.consumer.unassign.assert_not_called()
    assert worker.runtime.details['explicit_assignment']['stage'] == 'active'


@pytest.mark.parametrize('change', ['identity','deadline','early_acquire','early_resume'])
def test_invalid_command_cannot_release(worker, change):
    if change == 'identity': worker.command['incarnations']['c0']='replacement'
    if change == 'deadline': worker.command['deadline_epoch']=time.time()-1
    if change == 'early_acquire': worker.command['stage']='acquire'
    if change == 'early_resume': worker.command['stage']='resume'
    with pytest.raises(RuntimeError): worker.adapter.check()
    worker.consumer.unassign.assert_not_called()


def test_offset_mismatch_never_resumes(worker):
    worker.adapter.check()
    worker.command.update(stage='acquire',offsets={'0':17,'1':24})
    with pytest.raises(RuntimeError, match='Destination offset'): worker.adapter.check()
    worker.consumer.resume.assert_not_called()


def test_same_epoch_target_cannot_change(worker):
    worker.adapter.check()
    worker.command['ownership'].reverse()
    assert not worker.adapter.check()  # row ordering alone is harmless
    worker.command['ownership'][0]['owner']='c1'
    with pytest.raises(RuntimeError, match='changed within'): worker.adapter.check()


def test_evidence_flush_is_durable_and_not_a_json_record(tmp_path):
    writer=EvidenceWriter(tmp_path/'evidence.jsonl')
    try:
        writer.put({'completed':1})
        writer.flush()
        assert writer.path.read_text() == '{"completed":1}\n'
        writer.dropped=1
        with pytest.raises(RuntimeError): writer.flush()
    finally: writer.close()


class FakeAPI:
    def __init__(self, fault=None):
        self.control=dict(run_id='r', config_sha256='h', state='running', consumer_pods=['c0','c1'],
                          config={'NUM_PARTITIONS':'2','TOPIC_TITLE':'t'},producer_end_epoch=time.time()+1000)
        self.rows={n:dict(run_id='r', config_sha256='h',pod=n,incarnation='i'+str(i),assignment_mode='explicit',
                  phase='running',timestamp=time.time(),assignments=[['t_0',i]],explicit_assignment={'epoch':0,'stage':'active','offsets':{}})
                  for i,n in enumerate(['c0','c1'])}
        self.events=[];self.fault=fault
    def read_control(self): return copy.deepcopy(self.control)
    def pods(self, role): return ['c0','c1']
    def status(self, role, name): return copy.deepcopy(self.rows[name])
    def journal(self, control, filename, row): self.events.append(row['event'])
    def publish(self, control):
        self.control=copy.deepcopy(control)
        cmd=control['explicit_handoff'];stage=cmd['stage']
        for i,(name,row) in enumerate(self.rows.items()):
            if stage=='failed': continue
            if stage=='release':
                row['assignments']=[]
                row['explicit_assignment']=dict(epoch=1,stage='released',offsets={str(i):10+i})
                if self.fault=='restart': row['incarnation']='new'
                if self.fault=='unreleased': row['assignments']=[['t_0',i]]
            elif stage=='acquire':
                parts=[r['partition'] for r in cmd['ownership'] if r['owner']==name]
                row['assignments']=[['t_0',p] for p in parts]
                row['explicit_assignment']=dict(epoch=1,stage='acquired',offsets={str(p):cmd['offsets'][str(p)] for p in parts})
                if self.fault=='offset': row['explicit_assignment']['offsets']={}
            elif stage=='resume': row['explicit_assignment']['stage']='active'


def test_coordinator_requires_complete_release_acquire_barriers():
    api=FakeAPI()
    result=transfer(api,api.control,[dict(partition=0,owner='c1'),dict(partition=1,owner='c0')])
    assert result['status']=='resumed'
    assert api.events==['release_requested','released_verified','acquire_requested','acquired_verified','resume_requested','active_verified']


@pytest.mark.parametrize('fault',['restart','unreleased','offset'])
def test_coordinator_aborts_ambiguous_handoff(fault):
    api=FakeAPI(fault)
    with pytest.raises(RuntimeError): transfer(api,api.control,[dict(partition=0,owner='c1'),dict(partition=1,owner='c0')])
    assert api.control['state']=='failed'
    assert 'resume_requested' not in api.events


def test_healthy_unchanged_ownership_does_not_pause_consumers():
    api=FakeAPI()
    result=transfer(api,api.control,[dict(partition=0,owner='c0'),dict(partition=1,owner='c1')])
    assert result['status']=='no_action'
    assert 'explicit_handoff' not in api.control


def test_runner_rejects_redistribution_without_explicit_mode(monkeypatch):
    import run_experiment as runner
    monkeypatch.setattr(runner, 'validate_config', lambda config: (600,120,60,180))
    with pytest.raises(ValueError, match='explicit membership'):
        runner.validate_intervention({'action':'redistribute'}, {})


def test_runner_rejects_scaling_with_explicit_membership(monkeypatch):
    import run_experiment as runner
    monkeypatch.setattr(runner, 'validate_config', lambda config: (600,120,60,180))
    config=dict(CONSUMER_ASSIGNMENT_MODE='explicit',CONSUMER_POD_COUNT='2',TOPIC_COUNT='1',NUM_PARTITIONS='2',
                EXPLICIT_ASSIGNMENT_JSON=json.dumps([dict(partition=0,owner='consumer-sts-0'),dict(partition=1,owner='consumer-sts-1')]))
    with pytest.raises(ValueError, match='fixed membership'):
        runner.validate_intervention({'action':'scale'},config)


def test_stale_status_cannot_start_handoff():
    api=FakeAPI(); api.rows['c0']['timestamp']=time.time()-30
    with pytest.raises(RuntimeError, match='stale'):
        transfer(api,api.control,[dict(partition=0,owner='c1'),dict(partition=1,owner='c0')])
    assert 'explicit_handoff' not in api.control
