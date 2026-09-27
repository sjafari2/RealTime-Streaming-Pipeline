#!/usr/bin/env python3
"""Validate a nonempty handoff, then compare keep-three with scale-and-redistribute."""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys
import time
import yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'my-shell'))
import run_experiment as r
from placement_control import pod_identity
from validate import validate
HERE=Path(__file__).resolve().parent
TARGET=[dict(partition=p,owner='consumer-sts-'+str(p%6)) for p in range(60)]
ORDER=[('keep3',1,71),('scale6',1,71),('scale6',2,72),('keep3',2,72)]


def prepared(arm, number, seed, gate=False):
    config=yaml.safe_load((ROOT/'experiments/hot-ownership-20260927/concentrated-c2.yaml').read_text())
    data=config['data']
    data.update(EXP_ID=('c2-scale-validation' if gate else 'c2-'+arm+'-run'+str(number)),WORKLOAD_SEED=str(seed))
    data['CONSUMER_GROUP_ID']=data['EXP_ID']+'-'+str(time.time_ns())
    if gate:data.update(EXP_DURATION_SEC='240',WARMUP_SECONDS='20',DRAIN_SECONDS='60')
    plan=dict(action='scale_redistribute' if arm=='scale6' else 'none',initial_consumers=3,
              target_consumers=6 if arm=='scale6' else None,after_evaluation_start_seconds=20 if gate else 60)
    if arm=='scale6':plan['target_assignment']=TARGET
    r.validate_config(data);r.validate_intervention(plan,data)
    return config,plan


def normalized(raw):
    config=yaml.safe_load(raw)
    for key in ('RUN_ID','TOPIC_TITLE'):config['data'].pop(key,None)
    return config


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute',action='store_true')
    parser.add_argument('--gate-only',action='store_true');args=parser.parse_args()
    if not args.execute:
        print(json.dumps(dict(aggregate_rate=700,initial_hot_owner='consumer-sts-2',
          hot_partitions=list(range(12)),target_hot_counts=[2]*6,performance_order=ORDER,
          performance_phases_seconds=[60,300,120],action_after_evaluation_seconds=60,
          validation_phases_seconds=[20,220,60],validation_excluded_from_performance_count=True),indent=2));return
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed source before execution')
    if subprocess.check_output(['kubectl','config','current-context']).decode().strip()!='nautilus' or r.NS!='kafkastreamingdata':
        raise RuntimeError('Unexpected cluster or namespace')
    audit=ROOT/'results'/('c2-scaling-'+time.strftime('%Y%m%d-%H%M%S'));audit.mkdir(parents=True,exist_ok=False)
    state=dict(status='preparing',runs=[],validation=None,code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT).decode().strip())
    def save():
        p=audit/'campaign-status.tmp';p.write_text(json.dumps(state,indent=2)+'\n');p.replace(audit/'campaign-status.json')
    save();print('[CAMPAIGN]',audit,flush=True)
    with r.command_lock():
        c=r.read_control()
        if c and c.get('state') in ('preparing','running'):raise RuntimeError('Another run is active')
        original=r.shared_read(r.CONFIG);(audit/'original-config.yaml').write_bytes(original)
        original_count=json.loads(r.kubectl('get','statefulset','consumer-sts','-o','json'))['spec']['replicas']
        if original_count!=3:raise RuntimeError('Reviewed starting configuration requires three consumers')
        items=json.loads(r.kubectl('get','pods','-l','app in (producer-sts,consumer-sts)','-o','json'))['items']
        reference=dict(schema_version=1,pods=[pod_identity(p) for p in items],assignment=[dict(topic_index=0,partition=x['partition'],pod=x['owner']) for x in json.loads(prepared('keep3',1,71)[0]['data']['EXPLICIT_ASSIGNMENT_JSON'])])
        (audit/'starting-reference.json').write_text(json.dumps(reference,indent=2)+'\n')
        expected=None
        try:
            r.stop()
            config,plan=prepared('scale6',0,71,True)
            expected=yaml.safe_dump(config,sort_keys=False).encode();r.shared_write(r.CONFIG,expected)
            with r.paused_for_experiment(r,plan),r.prometheus_connection():
                jobs=[('scale6',0,71,True)]+([] if args.gate_only else [(a,n,s,False) for a,n,s in ORDER])
                for arm,number,seed,gate in jobs:
                    r.stop();r.set_consumer_baseline(3,180)
                    if normalized(r.shared_read(r.CONFIG))!=normalized(expected):raise RuntimeError('Shared configuration changed externally')
                    config,plan=prepared(arm,number,seed,gate);plan['placement_reference']=reference
                    expected=yaml.safe_dump(config,sort_keys=False).encode();r.shared_write(r.CONFIG,expected)
                    label='validation' if gate else arm+'-run'+str(number)
                    (audit/(label+'.yaml')).write_bytes(expected)
                    (audit/(label+'-plan.json')).write_text(json.dumps(plan,indent=2)+'\n')
                    r.preflight();state.update(status='running',current=label);save()
                    directory=r.complete_run(plan)
                    checked=validate(directory,require_all=gate)
                    row=dict(arm=arm,run_number=number,seed=seed,directory=str(directory),run_id=checked['run_id'],validation=checked)
                    if gate:state['validation']=row
                    else:state['runs'].append(row)
                    save();print('[VERIFIED]',label,checked['run_id'],flush=True)
                r.stop();r.set_consumer_baseline(original_count,180)
            state['status']='complete'
        except BaseException as exc:
            state.update(status='failed',error=str(exc) or type(exc).__name__);raise
        finally:
            try:
                r.stop();r.set_consumer_baseline(original_count,180)
                if expected is not None and normalized(r.shared_read(r.CONFIG))!=normalized(expected):raise RuntimeError('Shared settings changed externally; refusing restoration')
                r.shared_write(r.CONFIG,original)
                if r.shared_read(r.CONFIG)!=original:raise RuntimeError('Restoration differs from backup')
                state['restoration']='verified'
            except BaseException as exc:
                state.update(status='failed',restoration_error=str(exc));raise
            finally:state['finished_epoch']=time.time();save()


if __name__=='__main__':main()
