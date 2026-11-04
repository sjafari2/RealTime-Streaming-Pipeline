"""A stressed comparison must stop when its placement or first control is unsuitable."""
import copy
import importlib.util
import json
import os
from contextlib import nullcontext
from pathlib import Path
import sys

import pytest

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'my-shell'))
sys.path.insert(0,str(ROOT/'my-shell'))
from comparison_admission import (AdmissionStopped, deployment_check, load_plan,
                                  stress_check, validate_stress_settings)

SETTINGS=dict(window_seconds=60,consecutive_windows=3,minimum_growth_offsets_per_second=10,
              minimum_mean_backlog_offsets=1000,minimum_coverage=.95,maximum_gap_seconds=3)


def observations(function):
    return [dict(timestamp=t,total_lag=function(t),valid=True,owners={'0':'consumer-sts-0/process'})
            for t in range(0,601,2)]


def manifest():
    return dict(run_id='control',evaluation_start_epoch=0,producer_end_epoch=600,
                intervention={'action':'none'})


def test_clear_and_draining_controls_do_not_qualify_as_stress():
    for values in [lambda t:30+(t%6),lambda t:60000-50*t]:
        r=stress_check(manifest(),observations(values),SETTINGS)
        assert r['status']=='stopped_for_review' and r['reason']=='stress_not_demonstrated'


def test_startup_burst_does_not_qualify_when_later_windows_are_flat():
    r=stress_check(manifest(),observations(lambda t:200*t if t<100 else 30000),SETTINGS)
    assert r['reason']=='stress_not_demonstrated'


def test_growth_must_persist_in_each_window_not_only_over_the_entire_period():
    r=stress_check(manifest(),observations(lambda t:20*t if t<480 else 9600),SETTINGS)
    assert r['windows'][0]['passed'] and not r['windows'][1]['passed']
    assert r['status']=='stopped_for_review'


def test_valid_sustained_growth_passes_and_drain_is_excluded():
    rows=observations(lambda t:40*t)
    rows += [dict(timestamp=t,total_lag=0,valid=True,owners={'0':'consumer-sts-0/process'})
             for t in range(602,722,2)]
    r=stress_check(manifest(),rows,SETTINGS)
    assert r['status']=='passed'
    assert all(w['growth_offsets_per_second']==40 for w in r['windows'])


@pytest.mark.parametrize('kind',['missing','invalid','ownership','duplicate','nonfinite'])
def test_incomplete_or_inconsistent_monitoring_stops_instead_of_assuming_zero(kind):
    rows=observations(lambda t:40*t)
    if kind=='missing':rows=[r for r in rows if not 450<=r['timestamp']<=460]
    elif kind=='invalid':rows[230]['valid']=False
    elif kind=='ownership':rows[230]['owners']={'0':'consumer-sts-1/new'}
    elif kind=='duplicate':rows.insert(230,copy.deepcopy(rows[230]))
    else:rows[230]['total_lag']=float('nan')
    r=stress_check(manifest(),rows,SETTINGS)
    assert r['status']=='stopped_for_review' and r['reason']=='monitoring_insufficient'


@pytest.mark.parametrize('change',[{'minimum_growth_offsets_per_second':0},{'minimum_coverage':.5},
                                  {'consecutive_windows':1},{'window_seconds':float('nan')}])
def test_weak_or_invalid_thresholds_are_rejected(change):
    with pytest.raises(ValueError):validate_stress_settings(dict(SETTINGS,**change))


def pod(name,node,role='consumer'):
    return dict(pod=name,node=node,role=role,uid=name,container_id=name,image_id='image',restart_count=0,
                resources={'requests':{'cpu':'2','memory':'768Mi'},'limits':{'cpu':'2400m'}})


def deployment():
    pods=[pod('consumer-sts-0','slow'),pod('consumer-sts-2','fast'),pod('producer-workers-0','producer','producer')]
    admission=dict(reference_pods=copy.deepcopy(pods),hot_owner='consumer-sts-0',reference_hot_node='slow',
                   reference_run_id='earlier',processing_settings={'APP_CPU_ITERATIONS':'2000'})
    return admission,pods,{'APP_CPU_ITERATIONS':'2000'}


def test_current_reference_cannot_hide_a_different_bottleneck_node():
    a,p,c=deployment();a['hot_owner']='consumer-sts-2'
    r=deployment_check(a,p,c)
    assert r['status']=='stopped_for_review' and r['observed_hot_node']=='fast'


def test_recreated_pods_are_allowed_but_changed_nodes_or_resources_are_not():
    a,p,c=deployment();p[0]['uid']='new-uid';p[0]['container_id']='new-container'
    assert deployment_check(a,p,c)['status']=='passed'
    p[0]['node']='another-machine'
    assert deployment_check(a,p,c)['status']=='stopped_for_review'
    a,p,c=deployment();p[1]['resources']['requests']['cpu']='1'
    assert deployment_check(a,p,c)['status']=='stopped_for_review'


def test_producer_placement_and_processing_cost_are_also_compared():
    a,p,c=deployment();p[-1]['node']='other-producer-node'
    assert deployment_check(a,p,c)['status']=='stopped_for_review'
    a,p,c=deployment();c['APP_CPU_ITERATIONS']='1000'
    assert deployment_check(a,p,c)['processing_differences']


def test_plan_freezes_reference_identity_and_rejects_a_mitigation_reference(tmp_path):
    a,p,c=deployment()
    config=dict(c,PRODUCER_POD_COUNT='1',CONSUMER_POD_COUNT='2',TOPIC_COUNT='1',
                NUM_PARTITIONS='2',TRAFFIC_MODE='skew',SKEW_PARTITION='0',PAYLOAD_SIZE_BYTES='100')
    old=dict(manifest(),config=config,placement_reference={'schema_version':1,'pods':p,
        'assignment':[{'topic_index':0,'partition':0,'pod':'consumer-sts-0'},
                      {'topic_index':0,'partition':1,'pod':'consumer-sts-2'}]})
    reference=tmp_path/'earlier';reference.mkdir()
    (reference/'manifest.json').write_text(json.dumps(old))
    path=tmp_path/'plan.json'
    path.write_text(json.dumps({'schema_version':1,'purpose':'stressed_skew_comparison',
                               'reference_run':'earlier','hot_owner':'consumer-sts-0','stress':SETTINGS}))
    admitted=load_plan(path,tmp_path)
    assert admitted['reference_hot_node']=='slow'
    assert len(admitted['reference_manifest_sha256'])==64
    old['intervention']['action']='scale'
    (reference/'manifest.json').write_text(json.dumps(old))
    with pytest.raises(ValueError,match='no-mitigation'):load_plan(path,tmp_path)


def runner():
    source=Path(os.environ.get('CAPACITY_RUN_SCRIPT',ROOT/'experiments/four-condition-20260929/run.py'))
    spec=importlib.util.spec_from_file_location('admission_runner',source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_missing_plan_rejects_execution_before_any_cluster_call(monkeypatch):
    m=runner()
    monkeypatch.setattr(sys,'argv',['run.py','--execute'])
    monkeypatch.setattr(m.r,'kubectl',lambda *a,**k:pytest.fail('must not contact cluster'))
    with pytest.raises(ValueError,match='admission-plan'):m.main()


def test_first_clear_control_stops_batch_and_restores_configuration(monkeypatch,tmp_path):
    m=runner()
    protocol=m.campaign_protocol()
    monkeypatch.setattr(m,'campaign_protocol',lambda *a:copy.deepcopy(protocol))
    monkeypatch.setattr(m,'ROOT',tmp_path)
    (tmp_path/'results').mkdir()
    gate=tmp_path/'monitoring.json';gate.write_text('{"status":"passed"}')
    a,p,c=deployment();a['stress']=SETTINGS
    monkeypatch.setattr(m,'load_plan',lambda *args:a)
    monkeypatch.setattr(m,'deployment_check',lambda *args:{'status':'passed'})
    monkeypatch.setattr(sys,'argv',['run.py','--execute','--admission-plan','declared.json','--monitoring-gate',str(gate)])
    monkeypatch.setattr(m.subprocess,'check_output',lambda args,**k: b'nautilus' if args[:2]==['kubectl','config'] else (b'' if 'status' in args else b'revision'))
    monkeypatch.setattr(m.shutil,'which',lambda name:None)
    class Observer:
        def __init__(self,*a,**k):pass
        def wait(self,**k):return 0
    monkeypatch.setattr(m.subprocess,'Popen',Observer)
    monkeypatch.setattr(m.r,'command_lock',nullcontext)
    monkeypatch.setattr(m.r,'prometheus_connection',nullcontext)
    monkeypatch.setattr(m.r,'paused_for_experiment',lambda *a:nullcontext())
    monkeypatch.setattr(m.r,'read_control',lambda:None)
    original=b'data: {}\n';shared={'value':original}
    monkeypatch.setattr(m.r,'shared_read',lambda path:shared['value'])
    monkeypatch.setattr(m.r,'shared_write',lambda path,value:shared.update(value=value))
    stopped=[]
    monkeypatch.setattr(m.r,'stop',lambda:stopped.append(True))
    monkeypatch.setattr(m.r,'set_consumer_baseline',lambda *a:None)
    monkeypatch.setattr(m.r,'preflight',lambda:None)
    monkeypatch.setattr(m.r,'kubectl',lambda *a,**k:json.dumps({'spec':{'replicas':3}} if 'statefulset' in a else {'items':p}).encode())
    monkeypatch.setattr(m,'pod_identity',lambda p:p)
    ref={'assignment':[{'topic_index':0,'partition':i,'pod':'consumer-sts-'+str(i//20)} for i in range(60)]}
    executed=[]
    def prepared(arm,number,seed,*args,**kwargs):
        return {'data':{}},{'action':'scale' if kwargs.get('gate') else 'none','test_arm':arm,'test_number':number}
    monkeypatch.setattr(m,'prepared',prepared)
    def complete(plan):
        d=tmp_path/'results'/('trial-'+str(len(executed)));d.mkdir()
        executed.append(plan.copy())
        (d/'manifest.json').write_text(json.dumps({'run_id':d.name,'placement_reference':ref}))
        (d/'lag-summary.json').write_text(json.dumps({'snapshots':[{'valid':True,'timestamp':0,'owners':{str(i):'c'+str(i) for i in range(6)}}]}))
        return d
    monkeypatch.setattr(m.r,'complete_run',complete)
    monkeypatch.setattr(m,'audit_preparation',lambda d:{'valid':True,'issues':[]})
    monkeypatch.setattr(m,'validate',lambda *a,**k:{'status':'passed'})
    report=stress_check(manifest(),observations(lambda t:30),SETTINGS)
    monkeypatch.setattr(m,'check_saved_control',lambda *a:report)
    with pytest.raises(AdmissionStopped,match='Do not start'):m.main()
    assert [(x['test_arm'],x['test_number']) for x in executed if x['test_number']>0]==[('keep3',1)]
    assert shared['value']==original and stopped
    state=json.loads(next((tmp_path/'results').glob('four-condition-*/campaign-status.json')).read_text())
    assert state['status']=='stopped_for_review' and state['restoration']=='verified'
    assert len(state['runs'])==1 and state['runs'][0]['validation']['status']=='passed'
    assert next((tmp_path/'results').glob('four-condition-*/attention-required.json')).exists()


def test_resume_and_direct_dispatch_cannot_bypass_a_failed_stress_check():
    m=runner()
    with pytest.raises(AdmissionStopped):m.require_comparison_ready({'runs':[]},'redistribute3')
    with pytest.raises(AdmissionStopped):m.require_comparison_ready({'runs':[{}],'stress_admission':{'status':'stopped_for_review'}},'redistribute3')
    protocol=m.campaign_protocol()
    protocol['comparison_admission']={'stress':SETTINGS}
    state=dict(status='failed',restoration='verified',cost_protocol=protocol,
               monitoring_gate={'status':'passed'},runs=[dict(arm='keep3',run_number=1,seed=81,
               validation={'status':'passed'})],technical=[{'validation':{'status':'passed'}}],
               reference={'assignment':'frozen'},hot_partitions=list(range(12)))
    with pytest.raises(ValueError,match='stress check'):m.validate_resume_state(state,protocol)
    state['stress_admission']={'status':'passed'}
    assert m.validate_resume_state(state,protocol)==1
    state['status']='stopped_for_review'
    with pytest.raises(ValueError):m.validate_resume_state(state,protocol)


def test_starting_backlog_labels_still_use_their_first_control():
    m=runner()
    assert m.is_control('smaller__keep3') and m.is_control('larger__keep3')
    assert not m.is_control('smaller__redistribute3')
    m.require_comparison_ready({'runs':[]},'smaller__keep3')
    with pytest.raises(AdmissionStopped):
        m.require_comparison_ready({'runs':[{}]},'smaller__redistribute3')
