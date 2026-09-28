"""Reconcile full-run identities, including warm-up and ownership transitions."""
import importlib.util
import json
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('live_handoff_audit', ROOT/'experiments/c2-scaling-20260927/validate.py')
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)


def evidence(tmp_path, fault=None):
    def write(name,obj):
        p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(obj))
    write('manifest.json',dict(run_id='r',intervention={'action':'scale_redistribute'},explicit_scale_result={'status':'resumed'}))
    write('outcome-summary.json',dict(validity_failures=[],failed_or_cancelled_sends_whole_run=0,unresolved_sends_whole_run=0))
    write('lag-summary.json',dict(snapshots=[dict(valid=True,timestamp=97,processing_backlog=400)]))
    events=[];c0=[];c1=[]
    for p in range(2):
        for o in range(2):
            e=dict(message_id=str(p)+'-'+str(o),partition=p,offset=o)
            events.append(dict(e,event='acknowledged'))
            done=dict(e,event='completed',completion_timestamp=90 if o==0 else 110)
            dest=c1 if p==1 and o==1 else c0
            if not(fault=='missing' and p==1 and o==1):dest.append(done)
            if fault=='duplicate' and p==0 and o==0:dest.append(done)
    for role,pod,rows in [('producer','p0',events),('consumer','c0',c0),('consumer','c1',c1)]:
        name=role+'/'+pod+'/inc'
        write(name+'/final.json',dict(pod=pod))
        p=tmp_path/name/'events.jsonl';p.write_text(''.join(json.dumps(row)+'\n' for row in rows))
    stages=['release_requested','released_verified','acquire_requested','acquired_verified','resume_requested','active_verified']
    target=[dict(partition=0,owner='c0'),dict(partition=1,owner='c1')]
    transitions=[dict(event=name,timestamp=t) for name,t in zip(stages,[98,100,101,103,105,107])]
    transitions[1]['statuses']={'c0':{'explicit_assignment':{'offsets':{'0':1,'1':1}}},'c1':{'explicit_assignment':{'offsets':{}}}}
    transitions[2]['command']={'ownership':target}
    if fault=='offset':transitions[1]['statuses']['c0']['explicit_assignment']['offsets']['1']=2
    (tmp_path/'explicit-handoff-events.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in transitions))
    return tmp_path


def test_valid_live_barrier_reconciles_all_messages(tmp_path):
    result=v.validate(evidence(tmp_path),require_all=True)
    assert result['acknowledged_whole_run']==result['completion_attempts_whole_run']==4
    assert result['status']=='passed'


@pytest.mark.parametrize('fault',['missing','duplicate','offset'])
def test_failed_identity_or_offset_evidence_blocks_gate(tmp_path,fault):
    with pytest.raises(RuntimeError):v.validate(evidence(tmp_path,fault),require_all=True)
    assert json.loads((tmp_path/'handoff-validation.json').read_text())['status']=='failed'


@pytest.mark.parametrize('fault',[None,'offset'])
def test_redistribution_without_scaling_still_checks_handoff(tmp_path,fault):
    directory=evidence(tmp_path,fault)
    p=directory/'manifest.json';manifest=json.loads(p.read_text())
    manifest['intervention']['action']='redistribute'
    manifest.pop('explicit_scale_result')
    p.write_text(json.dumps(manifest))
    if fault:
        with pytest.raises(RuntimeError):v.validate(directory,require_all=True)
    else:
        assert v.validate(directory,require_all=True)['handoff_checked']
