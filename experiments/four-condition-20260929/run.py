#!/usr/bin/env python3
"""Compare four scheduled responses from a verified common hot-owner assignment."""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'my-shell'))
import run_experiment as r
from placement_control import PlacementMismatch,pod_identity
from static_startup import audit_preparation
sys.path.insert(0,str(ROOT/'experiments/c2-scaling-20260927'))
from validate import validate
HPA_PLAN=dict(action='scale',initial_consumers=3,target_consumers=6,after_evaluation_start_seconds=60)
ORDER=[('keep3',1,81),('redistribute3',1,81),('scale_redistribute6',1,81),('kafka_scale6',1,81),
       ('kafka_scale6',2,82),('scale_redistribute6',2,82),('redistribute3',2,82),('keep3',2,82)]


def target_map(hot,count):
    cold=sorted(set(range(60))-set(hot))
    return sorted([dict(partition=p,owner='consumer-sts-'+str(i%count))
                   for group in [sorted(hot),cold] for i,p in enumerate(group)],key=lambda x:x['partition'])


def prepared(arm,number,seed,reference=None,hot=None,gate=False):
    config=yaml.safe_load((ROOT/'experiments/hot-ownership-20260927/concentrated-c2.yaml').read_text())
    data=config['data'];native=arm=='kafka_scale6'
    data.update(EXP_ID='four-condition-'+arm+('-technical' if gate else '-run'+str(number)),
                WORKLOAD_SEED=str(seed),EXP_DURATION_SEC='240' if gate else '660',
                WARMUP_SECONDS='20' if gate else '60',DRAIN_SECONDS='60' if gate else '120',
                CONSUMER_ASSIGNMENT_MODE='cooperative' if native else 'explicit',
                CONSUMER_STATIC_MEMBERSHIP='true' if native else 'false')
    if gate and native:
        data.update(TARGET_RATE='100',EXP_DURATION_SEC='120')
    data['CONSUMER_GROUP_ID']=data['EXP_ID']+'-'+str(time.time_ns())
    if hot is not None:data['SKEW_PARTITION']=','.join(map(str,sorted(hot)))
    if native:data.pop('EXPLICIT_ASSIGNMENT_JSON',None)
    elif reference is not None:
        data['EXPLICIT_ASSIGNMENT_JSON']=json.dumps([dict(partition=x['partition'],owner=x['pod']) for x in reference['assignment']])
    action={'keep3':'none','redistribute3':'redistribute','scale_redistribute6':'scale_redistribute','kafka_scale6':'scale'}[arm]
    plan=dict(action=action,initial_consumers=3,target_consumers=6 if 'scale' in arm else None,
              after_evaluation_start_seconds=20 if gate else 60,
              recovery_threshold_offsets=100,recovery_hold_seconds=30)
    if arm in ('redistribute3','scale_redistribute6'):plan['target_assignment']=target_map(hot or list(range(12)),6 if arm=='scale_redistribute6' else 3)
    if reference is not None:plan['placement_reference']=reference
    r.validate_config(data);r.validate_intervention(plan,data)
    return config,plan


def normalized(raw):
    cfg=yaml.safe_load(raw)
    for key in ('RUN_ID','TOPIC_TITLE'):cfg['data'].pop(key,None)
    return cfg


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--monitoring-gate',type=Path)
    parser.add_argument('--preparations-only',action='store_true')
    args=parser.parse_args()
    if not args.execute:
        print(json.dumps(dict(aggregate_rate=700,partitions=60,hot_fraction=.8,hot_partition_count=12,
            initial_hot_owner='consumer-sts-2',phases_seconds=[60,600,120],order=ORDER,
            normal_scaling='classic cooperative-sticky with unique static member identities; no targeted map',
            starting_map='capture Kafka empty-topic map; freeze twelve partitions owned by consumer 2; verify all trials against it',
            preparation_retry_limit=3),indent=2));return
    if not args.monitoring_gate or json.loads(args.monitoring_gate.read_text())['status']!='passed':
        raise RuntimeError('A passed live monitoring correction gate is required')
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed source before execution')
    if subprocess.check_output(['kubectl','config','current-context']).decode().strip()!='nautilus' or r.NS!='kafkastreamingdata':
        raise RuntimeError('Unexpected cluster or namespace')
    audit=ROOT/'results'/('four-condition-'+time.strftime('%Y%m%d-%H%M%S'));audit.mkdir()
    state=dict(status='preparing',runs=[],preparations=[],technical=[],cost_protocol=json.loads((Path(__file__).parent/'protocol.json').read_text()),code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT).decode().strip(),monitoring_gate=json.loads(args.monitoring_gate.read_text()))
    def save():
        p=audit/'campaign-status.tmp';p.write_text(json.dumps(state,indent=2)+'\n');p.replace(audit/'campaign-status.json')
    (audit/'protocol.json').write_text(json.dumps(state['cost_protocol'],indent=2)+'\n')
    save();print('[CAMPAIGN]',audit,flush=True)
    if shutil.which('caffeinate'):subprocess.Popen(['caffeinate','-i','-w',str(os.getpid())])
    with r.command_lock():
        control=r.read_control()
        if control and control.get('state') in ('preparing','running'):raise RuntimeError('Another run is active')
        original=r.shared_read(r.CONFIG);(audit/'original-config.yaml').write_bytes(original)
        count=json.loads(r.kubectl('get','statefulset','consumer-sts','-o','json'))['spec']['replicas']
        if count!=3:raise RuntimeError('Three starting consumers are required')
        items=json.loads(r.kubectl('get','pods','-l','app in (producer-sts,consumer-sts)','-o','json'))['items']
        original_pods=[pod_identity(p) for p in items]
        expected=None;observer=None;observer_log=None
        def configure(config,label,plan):
            nonlocal expected
            if expected is not None and normalized(r.shared_read(r.CONFIG))!=normalized(expected):raise RuntimeError('External configuration change')
            expected=yaml.safe_dump(config,sort_keys=False).encode();r.shared_write(r.CONFIG,expected)
            (audit/(label+'.yaml')).write_bytes(expected)
            (audit/(label+'-plan.json')).write_text(json.dumps(plan,indent=2)+'\n')
        def run_one(config,plan,label,technical=False,preparation=False):
            # Only a pre-traffic ownership mismatch may be retried, with its evidence retained.
            for attempt in range(1,4):
                r.stop();r.set_consumer_baseline(3,180)
                config=copy.deepcopy(config)
                config['data']['CONSUMER_GROUP_ID']=label+'-'+str(time.time_ns())
                tag=label+'-attempt'+str(attempt)
                configure(config,tag,plan);r.preflight()
                state.update(status='running',current=tag);save()
                try:
                    with r.prometheus_connection():directory=r.complete_run(plan)
                except PlacementMismatch as exc:
                    current=r.read_control()
                    if current and 'start_epoch' in current:raise
                    state['preparations'].append(dict(label=tag,status='rejected_before_traffic',error=str(exc),run_id=(current or {}).get('run_id')));save()
                    if attempt==3:raise
                    continue
                m=json.loads((directory/'manifest.json').read_text())
                if preparation:
                    checked=audit_preparation(directory)
                    state['preparations'].append(dict(label=tag,directory=str(directory),validation=checked));save()
                else:
                    checked=validate(directory,require_all=technical)
                    if plan['action']=='scale':
                        lag=json.loads((directory/'lag-summary.json').read_text())
                        six=[x for x in lag['snapshots'] if x.get('valid') and
                             len({owner.split('/')[0] for owner in x['owners'].values()})==6]
                        if not six:raise RuntimeError('Kafka scale-up never demonstrated six active owners')
                        if technical and any(not x['valid'] for x in lag['snapshots'] if x['timestamp']>=six[0]['timestamp']+10):
                            raise RuntimeError('Native scaling monitoring became invalid after ownership settled')
                return directory,m,checked
            raise RuntimeError('No admitted preparation')
        try:
            observer_log=(audit/'resource-observer.log').open('w')
            observer=subprocess.Popen([sys.executable,str(ROOT/'my-shell/observe_resources.py'),str(audit)],stdout=observer_log,stderr=subprocess.STDOUT)
            r.stop()
            config,plan=prepared('kafka_scale6',0,81)
            plan.update(action='none',target_consumers=None,prepare_only=True,capture_placement_pods=original_pods)
            configure(config,'capture-initial',plan)
            with r.paused_for_experiment(r,HPA_PLAN):
                directory,m,_=run_one(config,plan,'capture-initial',preparation=True)
                reference=m['placement_reference']
                hot=sorted(x['partition'] for x in reference['assignment'] if x['pod']=='consumer-sts-2')[:12]
                counts={n:sum(x['pod']==n for x in reference['assignment']) for n in ['consumer-sts-0','consumer-sts-1','consumer-sts-2']}
                if len(hot)!=12 or set(counts.values())!={20}:raise RuntimeError('Initial Kafka map is not 20 partitions per consumer')
                state.update(reference=reference,hot_partitions=hot);save()
                (audit/'starting-reference.json').write_text(json.dumps(reference,indent=2)+'\n')
                config,plan=prepared('kafka_scale6',0,81,reference,hot)
                plan.update(action='none',target_consumers=None,prepare_only=True)
                run_one(config,plan,'verify-native-restart',preparation=True)
                if not args.preparations_only:
                    # Native rebalance correctness is checked before counting any performance trial.
                    config,plan=prepared('kafka_scale6',0,81,reference,hot,gate=True)
                    directory,m,checked=run_one(config,plan,'native-scale-technical',technical=True)
                    state['technical'].append(dict(directory=str(directory),validation=checked));save()
                    for arm,number,seed in ORDER:
                        if shutil.disk_usage(ROOT).free<4*1024**3:raise RuntimeError('Less than 4 GiB free; preserve evidence before continuing')
                        config,plan=prepared(arm,number,seed,reference,hot)
                        directory,m,checked=run_one(config,plan,arm+'-run'+str(number))
                        state['runs'].append(dict(arm=arm,run_number=number,seed=seed,directory=str(directory),run_id=m['run_id'],validation=checked));save()
                        print('[VERIFIED]',arm,number,m['run_id'],flush=True)
                r.stop();r.set_consumer_baseline(count,180)
            state['status']='complete'
        except BaseException as exc:
            state.update(status='failed',error=str(exc) or type(exc).__name__);raise
        finally:
            try:
                r.stop();r.set_consumer_baseline(count,180)
                if expected is not None and normalized(r.shared_read(r.CONFIG))!=normalized(expected):raise RuntimeError('External configuration change; restoration withheld')
                r.shared_write(r.CONFIG,original)
                if r.shared_read(r.CONFIG)!=original:raise RuntimeError('Restoration differs from backup')
                state['restoration']='verified'
            except BaseException as exc:
                state.update(status='failed',restoration_error=str(exc));raise
            finally:
                state['finished_epoch']=time.time();save()
                if observer:
                    try:observer.wait(timeout=15)
                    except subprocess.TimeoutExpired:observer.terminate();observer.wait(timeout=5)
                if observer_log:observer_log.close()


if __name__=='__main__':main()
