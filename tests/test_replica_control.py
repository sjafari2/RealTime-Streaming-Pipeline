"""Fixed and scheduled trials must not silently run with an HPA's replica floor."""
import copy
import sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'my-shell'))
import run_experiment as r


def hpa(paused=False):
 return dict(metadata=dict(name='consumer-hpa',uid='known'),spec=dict(scaleTargetRef=dict(kind='StatefulSet',name='consumer-sts'),minReplicas=1,maxReplicas=12,behavior={direction:dict(selectPolicy='Disabled' if paused else 'Max') for direction in ('scaleUp','scaleDown')}))


def test_active_controller_is_rejected_even_at_current_replica_count():
 with pytest.raises(RuntimeError,match='can change experiment replicas'):
  r.validate_replica_control(dict(CONSUMER_POD_COUNT='6'),hpas=[hpa()])


def test_paused_controller_still_cannot_force_a_six_replica_floor():
 item=hpa(True);item['spec']['minReplicas']=6
 with pytest.raises(RuntimeError,match='bounds conflict'):
  r.validate_replica_control(dict(CONSUMER_POD_COUNT='3'),hpas=[item])


def test_paused_bounds_cover_both_ends_of_scheduled_scaling():
 config=dict(CONSUMER_POD_COUNT='3',PRODUCER_POD_COUNT='3');plan=dict(initial_consumers=3,target_consumers=6)
 assert len(r.validate_replica_control(config,plan,[hpa(True)]))==1
 item=hpa(True);item['spec']['maxReplicas']=5
 with pytest.raises(RuntimeError,match='bounds conflict'):r.validate_replica_control(config,plan,[item])
 unrelated=hpa();unrelated['spec']['scaleTargetRef']['name']='other-workload'
 assert r.validate_replica_control(config,plan,[unrelated])==[]


def test_actual_replicas_must_match_declared_initial_configuration():
 config=dict(CONSUMER_POD_COUNT='3',PRODUCER_POD_COUNT='3')
 with pytest.raises(RuntimeError,match='Initial replica mismatch'):
  r.validate_initial_replicas(config,['p0','p1','p2'],['c'+str(i) for i in range(6)])
 r.validate_initial_replicas(config,['p0','p1','p2'],['c0','c1','c2'],dict(initial_consumers=None))
