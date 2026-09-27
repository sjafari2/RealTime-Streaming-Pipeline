"""New replicas cannot fetch until their complete release/acquire barrier passes."""
import copy
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src/common'))
sys.path.insert(0, str(ROOT/'my-shell'))
from explicit_assignment import ExplicitAssignment
import explicit_scale as scale
import run_experiment as r


def make_newcomer(monkeypatch, change=None):
    initial=['c0','c1']; names=initial+['c2']
    state=dict(state='running',run_id='r',consumer_pods=initial,explicit_scale_consumers=names,
               explicit_enrollment=dict(run_id='r',members=names,deadline_epoch=time.time()+60))
    if change: change(state)
    monkeypatch.setenv('NUM_PARTITIONS','2')
    monkeypatch.setenv('EXPLICIT_ASSIGNMENT_JSON',json.dumps([dict(partition=p,owner=initial[p]) for p in range(2)]))
    monkeypatch.setenv('CONSUMER_STATIC_MEMBERSHIP','false')
    rt=SimpleNamespace(control_path='control',control=lambda:state,pod='c2',run_id='r',incarnation='i2',details={})
    w=SimpleNamespace(runtime=rt,topics=['t'],on_assign=Mock(),ownership_event=Mock(),topic_partition=Mock())
    return w,state


def test_new_replica_initializes_without_partition_or_broker_fetch(monkeypatch):
    w,state=make_newcomer(monkeypatch)
    adapter=ExplicitAssignment(w);adapter.initialize()
    w.on_assign.assert_not_called()
    w.ownership_event.assert_called_once_with('explicit_waiting',[])
    assert adapter.stage=='active' and adapter.epoch==0 and adapter.offsets=={}


@pytest.mark.parametrize('change',[
    lambda c:c.pop('explicit_enrollment'),
    lambda c:c['explicit_enrollment'].update(deadline_epoch=time.time()-1),
    lambda c:c['explicit_enrollment'].update(run_id='other'),
    lambda c:c.update(explicit_handoff={'stage':'acquire'}),
    lambda c:c.update(state='preparing'),
    lambda c:c.update(explicit_scale_consumers=['c0','c2']),
])
def test_new_replica_rejects_stale_or_unapproved_enrollment(monkeypatch,change):
    w,state=make_newcomer(monkeypatch,change)
    with pytest.raises(ValueError):ExplicitAssignment(w)
    w.on_assign.assert_not_called()


def test_old_consumer_restart_is_rejected_during_running_enrollment(monkeypatch):
    w,state=make_newcomer(monkeypatch);w.runtime.pod='c0'
    with pytest.raises(ValueError):ExplicitAssignment(w)


class ScaleAPI:
    def __init__(self,fault=None):
        self.control=dict(state='running',run_id='r',config_sha256='h',consumer_pods=['c0'],
            explicit_scale_consumers=['c0','c1'],explicit_start_verification={'incarnations':{'c0':'i0'}},
            intervention=dict(initial_consumers=1,target_consumers=2,target_assignment=[{'partition':0,'owner':'c1'}]),
            producer_end_epoch=time.time()+1000)
        self.names=['c0'];self.fault=fault;self.events=[]
    def read_control(self):return copy.deepcopy(self.control)
    def publish(self,c):self.control=copy.deepcopy(c)
    def pods(self,role):return self.names
    def kubectl(self,*args):self.names=['c0','c1']
    def resource_snapshot(self,c):pass
    def preflight(self):pass
    def clock_probes(self,*args):return []
    def journal(self,c,f,row):self.events.append(row['event'])
    def status(self,role,name):
        row=dict(run_id='r',config_sha256='h',pod=name,incarnation='i'+name[-1],assignment_mode='explicit',
                 phase='running',timestamp=time.time(),explicit_assignment=dict(epoch=0,stage='active'),
                 assignments=[['t',0]] if name=='c0' else [])
        if self.fault=='restart' and name=='c0':row['incarnation']='restarted'
        if self.fault=='early_claim' and name=='c1':row['assignments']=[['t',0]]
        if self.fault=='failure' and name=='c1':row['failure']='disk'
        return row


def test_scale_checks_empty_newcomers_before_transfer(monkeypatch):
    api=ScaleAPI(); handoff=Mock(return_value={'status':'resumed'})
    monkeypatch.setattr(scale,'transfer',handoff)
    scale.scale_and_transfer(api,api.control)
    assert handoff.call_count==1
    assert api.events==['replicas_requested','empty_enrollment_verified']
    assert api.control['explicit_scale_result']['status']=='resumed'


@pytest.mark.parametrize('fault',['restart','early_claim','failure'])
def test_bad_enrollment_stops_run_without_transfer(monkeypatch,fault):
    api=ScaleAPI(fault);handoff=Mock();monkeypatch.setattr(scale,'transfer',handoff)
    with pytest.raises(RuntimeError):scale.scale_and_transfer(api,api.control)
    handoff.assert_not_called();assert api.control['state']=='failed'


def test_failed_transfer_stops_managed_run(monkeypatch):
    api=ScaleAPI();monkeypatch.setattr(scale,'transfer',Mock(side_effect=RuntimeError('readback failed')))
    with pytest.raises(RuntimeError):scale.scale_and_transfer(api,api.control)
    assert api.control['state']=='failed'


def test_explicit_scale_plan_requires_complete_predeclared_target(monkeypatch):
    monkeypatch.setattr(r,'validate_config',lambda _: (360,120,60,180))
    config=dict(CONSUMER_ASSIGNMENT_MODE='explicit',CONSUMER_POD_COUNT='1',TOPIC_COUNT='1',NUM_PARTITIONS='2',
        EXPLICIT_ASSIGNMENT_JSON=json.dumps([dict(partition=p,owner='consumer-sts-0') for p in range(2)]))
    plan=dict(action='scale_redistribute',initial_consumers=1,target_consumers=2,after_evaluation_start_seconds=60,
        target_assignment=[dict(partition=0,owner='consumer-sts-0'),dict(partition=1,owner='consumer-sts-1')])
    r.validate_intervention(plan,config)
    plan['target_assignment'][1]['owner']='consumer-sts-0'
    with pytest.raises(ValueError,match='Every scaled'):r.validate_intervention(plan,config)
