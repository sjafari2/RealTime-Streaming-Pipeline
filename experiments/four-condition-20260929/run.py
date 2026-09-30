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
ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[2]))
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


def prepared(arm,number,seed,reference=None,hot=None,gate=False,aggregate_rate=700,hot_count=12,deadline_ms=99):
    if aggregate_rate not in (700,800):raise ValueError('Unreviewed aggregate rate')
    if hot_count not in (4,12):raise ValueError('Unreviewed hot-partition count')
    config=yaml.safe_load((ROOT/'experiments/hot-ownership-20260927/concentrated-c2.yaml').read_text())
    data=config['data'];native=arm=='kafka_scale6'
    prefix='four-condition-' if aggregate_rate==700 and hot_count==12 else 'hot'+str(hot_count)+'-rate'+str(aggregate_rate)+'-'
    data.update(EXP_ID=prefix+arm+('-technical' if gate else '-run'+str(number)),
                TARGET_RATE=format(aggregate_rate/3,'.17g'),SLO_THRESHOLD_MS=str(deadline_ms),
                WORKLOAD_SEED=str(seed),EXP_DURATION_SEC='240' if gate else '660',
                WARMUP_SECONDS='20' if gate else '60',DRAIN_SECONDS='60' if gate else '120',
                CONSUMER_ASSIGNMENT_MODE='cooperative' if native else 'explicit',
                CONSUMER_STATIC_MEMBERSHIP='true' if native else 'false')
    if gate and native:
        data.update(TARGET_RATE='100',EXP_DURATION_SEC='120')
    data['CONSUMER_GROUP_ID']=data['EXP_ID']+'-'+str(time.time_ns())
    if hot is None:hot=list(map(int,data['SKEW_PARTITION'].split(',')))[:hot_count]
    if len(hot)!=hot_count:raise ValueError('Hot set differs from the protocol')
    data['SKEW_PARTITION']=','.join(map(str,sorted(hot)))
    if native:data.pop('EXPLICIT_ASSIGNMENT_JSON',None)
    elif reference is not None:
        data['EXPLICIT_ASSIGNMENT_JSON']=json.dumps([dict(partition=x['partition'],owner=x['pod']) for x in reference['assignment']])
    action={'keep3':'none','redistribute3':'redistribute','scale_redistribute6':'scale_redistribute','kafka_scale6':'scale'}[arm]
    plan=dict(action=action,initial_consumers=3,target_consumers=6 if 'scale' in arm else None,
              after_evaluation_start_seconds=20 if gate else 60,
              recovery_threshold_offsets=100,recovery_hold_seconds=30)
    if arm in ('redistribute3','scale_redistribute6'):plan['target_assignment']=target_map(hot,6 if arm=='scale_redistribute6' else 3)
    if reference is not None:plan['placement_reference']=reference
    r.validate_config(data);r.validate_intervention(plan,data)
    return config,plan


def normalized(raw):
    cfg=yaml.safe_load(raw)
    for key in ('RUN_ID','TOPIC_TITLE'):cfg['data'].pop(key,None)
    return cfg


def campaign_protocol(aggregate_rate=700,targeted_only=False,hot_count=12,deadline_ms=99):
    if aggregate_rate not in (700,800):raise ValueError('Unreviewed aggregate rate')
    protocol=json.loads((ROOT/'experiments/four-condition-20260929/protocol.json').read_text())
    if hot_count not in (4,12):raise ValueError('Unreviewed hot-partition count')
    if deadline_ms not in (99,1000):raise ValueError('Unreviewed completion deadline')
    if aggregate_rate==700 and not targeted_only and hot_count==12 and deadline_ms==99:return protocol
    order=[row for row in ORDER if not targeted_only or row[0] in ('redistribute3','scale_redistribute6')]
    protocol.update(aggregate_input_rate=aggregate_rate,hot_partition_count=hot_count,completion_deadline_ms=deadline_ms,performance_trials=len(order),
                    order=[list(row) for row in order],
                    conditions={k:v for k,v in protocol['conditions'].items() if any(row[0]==k for row in order)})
    protocol['conditions'].update({k:('Keep three; distribute '+str(hot_count)+' hot partitions as evenly as whole partitions permit' if k=='redistribute3' else 'Scale to six; distribute '+str(hot_count)+' hot partitions as evenly as whole partitions permit') for k in protocol['conditions'] if k in ('redistribute3','scale_redistribute6')})
    protocol['starting_conditions']=protocol['starting_conditions'].replace('twelve lowest-numbered',str(hot_count)+' lowest-numbered')
    protocol['study_purpose']='Test partition granularity at a rate previously observed to sustain balanced input. Keep 700 messages/s for the first block and at most 800 for any follow-up; retain every valid outcome regardless of direction. Four hot partitions create a different skew pattern from 80/20 across twelve of sixty partitions.'
    protocol['completion_deadline_reporting_ms']=[500,1000] if deadline_ms==1000 else [deadline_ms]
    protocol['slo_rationale']='For the new trials, one second is the primary experimental completion deadline and 0.5 seconds is a prespecified stricter sensitivity threshold. Balanced 700 messages/s calibration recorded p99 of 0.248 and 0.395 seconds; these observations provide context but do not establish an application requirement or guarantee. Neither threshold is a validated application SLA. Historical 99 ms outcomes retain their original threshold.'
    protocol['deadline_outcome_definition']='At each threshold, count late earliest completions plus unfinished admitted evaluation messages whose deadlines have elapsed at the original observation cutoff; divide by distinct admitted evaluation messages. Report not-yet-expired deadlines as censored and suppress the full-cohort miss fraction if any remain censored or completion clocks are invalid. Completion is before commit acknowledgment. Equality with the deadline is on time.'
    protocol['lag_analysis_parameters']=dict(window_samples=15,growth_window_samples=5,export_step_seconds=2,hot_k=1.0,minimum_lag=10.0,persistence=0.8,max_gap_seconds=3.0)
    protocol['outcomes']=[text.replace('30-second growth','10-second growth') for text in protocol['outcomes']]
    protocol['assignment_limit']='The implemented target balances hot and cold partition counts separately. With four hot partitions, three consumers receive hot counts 2,1,1, while six receive 1,1,1,1,0,0. This is not an optimal load- or capacity-weighted three-consumer assignment; a different assignment may reduce any observed disadvantage.'
    protocol['primary_outcomes']=['Unfinished evaluation messages after fixed drain',
        'Confirmed recovery while input continues','Final-two-minute processing backlog growth',
        'Whole-evaluation-cohort completion p99 reported with unfinished work']
    protocol['secondary_outcomes']=['Final-two-minute production-cohort latency with unfinished work',
        'Processing interruption and transition accumulation','CPU and memory usage with coverage',
        'Requested CPU and memory over time','Throughput, lag, skew and ownership',
        'Admitted-cohort completion deadline misses at the primary and prespecified sensitivity thresholds']
    protocol['interpretation']='Compare the complete scheduled responses, including preparation and handover. A capacity-limited condition is a hypothesis, not a guaranteed favorable scaling result. The late cohort is a fixed-window outcome, not called post-recovery unless recovery actually precedes its start.'
    return protocol


def validate_resume_state(state, protocol):
    if state.get('status')!='failed' or state.get('restoration')!='verified':
        raise ValueError('Resume requires a stopped, restored failed campaign')
    if state.get('cost_protocol')!=protocol or state.get('monitoring_gate',{}).get('status')!='passed':
        raise ValueError('Resume cannot change the protocol or bypass the monitoring gate')
    completed=[(x['arm'],x['run_number'],x['seed']) for x in state['runs']]
    order=[tuple(row) for row in protocol['order']]
    if completed!=order[:len(completed)] or len(completed)>=len(order):
        raise ValueError('Completed trials must form an unrepeated prefix of the planned order')
    if any(x.get('validation',{}).get('status')!='passed' for x in state['runs']):
        raise ValueError('A completed trial lacks validation')
    if not state.get('technical') or any(x['validation']['status']!='passed' for x in state['technical']):
        raise ValueError('Native scaling validation is missing')
    if not state.get('reference') or len(state.get('hot_partitions',[]))!=protocol['hot_partition_count']:
        raise ValueError('Frozen starting conditions are missing')
    return len(completed)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--resume',type=Path,help='Continue only unexecuted trials in a restored campaign')
    parser.add_argument('--monitoring-gate',type=Path)
    parser.add_argument('--preparations-only',action='store_true')
    parser.add_argument('--aggregate-rate',type=int,choices=(700,800),default=700,
                        help='Total messages/s across all three producers')
    parser.add_argument('--targeted-only',action='store_true',
                        help='Compare redistribution within three with scaling plus redistribution, twice each')
    parser.add_argument('--hot-partitions',type=int,choices=(4,12),default=12)
    parser.add_argument('--slo-ms',type=int,choices=(99,1000),default=None)
    args=parser.parse_args()
    deadline_ms=args.slo_ms if args.slo_ms is not None else (1000 if args.hot_partitions==4 else 99)
    protocol=campaign_protocol(args.aggregate_rate,args.targeted_only,args.hot_partitions,deadline_ms)
    order=[tuple(row) for row in protocol['order']]
    if not args.execute:
        print(json.dumps(dict(aggregate_rate=args.aggregate_rate,partitions=60,hot_fraction=.8,hot_partition_count=args.hot_partitions,completion_deadline_ms=deadline_ms,
            completion_deadline_reporting_ms=protocol.get('completion_deadline_reporting_ms',[deadline_ms]),
            initial_hot_owner='consumer-sts-2',phases_seconds=[60,600,120],order=order,
            normal_scaling='technical validation only' if args.targeted_only else 'classic cooperative-sticky with unique static member identities; no targeted map',
            starting_map='capture Kafka empty-topic map; freeze the selected hot partitions owned by consumer 2; verify all trials against it',
            preparation_retry_limit=3),indent=2));return
    if not args.monitoring_gate or json.loads(args.monitoring_gate.read_text())['status']!='passed':
        raise RuntimeError('A passed live monitoring correction gate is required')
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed source before execution')
    if subprocess.check_output(['kubectl','config','current-context']).decode().strip()!='nautilus' or r.NS!='kafkastreamingdata':
        raise RuntimeError('Unexpected cluster or namespace')
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT).decode().strip()
    if args.resume:
        if args.preparations_only:raise ValueError('Resume cannot repeat preparation-only stages')
        audit=args.resume.resolve();state=json.loads((audit/'campaign-status.json').read_text())
        validate_resume_state(state,protocol)
        state.setdefault('resume_events',[]).append(dict(timestamp=time.time(),revision=revision,prior_error=state.get('error'),completed_trials=len(state['runs'])))
        state.update(status='preparing');state.pop('error',None);state.pop('finished_epoch',None)
    else:
        audit=ROOT/'results'/('four-condition-'+time.strftime('%Y%m%d-%H%M%S'));audit.mkdir()
        state=dict(status='preparing',runs=[],preparations=[],technical=[],cost_protocol=protocol,code_commit=revision,monitoring_gate=json.loads(args.monitoring_gate.read_text()))
    def save():
        p=audit/'campaign-status.tmp';p.write_text(json.dumps(state,indent=2)+'\n');p.replace(audit/'campaign-status.json')
    (audit/'protocol.json').write_text(json.dumps(state['cost_protocol'],indent=2)+'\n')
    save();print('[CAMPAIGN]',audit,flush=True)
    if shutil.which('caffeinate'):subprocess.Popen(['caffeinate','-is','-w',str(os.getpid())])
    with r.command_lock():
        control=r.read_control()
        if control and control.get('state') in ('preparing','running'):raise RuntimeError('Another run is active')
        original=r.shared_read(r.CONFIG)
        if args.resume:
            if original!=(audit/'original-config.yaml').read_bytes():raise RuntimeError('Shared configuration changed since restoration')
        else:(audit/'original-config.yaml').write_bytes(original)
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
            observer_dir=audit
            if args.resume:
                observer_dir=audit/('observer-resume-'+time.strftime('%Y%m%d-%H%M%S'));observer_dir.mkdir()
                (observer_dir/'campaign-status.json').symlink_to(audit/'campaign-status.json')
                state.setdefault('observer_segments',['.']).append(observer_dir.name);save()
            observer_log=(observer_dir/'resource-observer.log').open('w')
            observer=subprocess.Popen([sys.executable,str(ROOT/'my-shell/observe_resources.py'),str(observer_dir)],stdout=observer_log,stderr=subprocess.STDOUT)
            r.stop()
            config,plan=prepared('kafka_scale6',0,81,aggregate_rate=args.aggregate_rate,hot_count=args.hot_partitions,deadline_ms=deadline_ms)
            plan.update(action='none',target_consumers=None,prepare_only=True,capture_placement_pods=original_pods)
            configure(config,'resume-setup-'+time.strftime('%Y%m%d-%H%M%S') if args.resume else 'capture-initial',plan)
            with r.paused_for_experiment(r,HPA_PLAN):
                if args.resume:
                    reference=state['reference'];hot=state['hot_partitions']
                else:
                    directory,m,_=run_one(config,plan,'capture-initial',preparation=True)
                    reference=m['placement_reference']
                    hot=sorted(x['partition'] for x in reference['assignment'] if x['pod']=='consumer-sts-2')[:args.hot_partitions]
                    counts={n:sum(x['pod']==n for x in reference['assignment']) for n in ['consumer-sts-0','consumer-sts-1','consumer-sts-2']}
                    if len(hot)!=args.hot_partitions or set(counts.values())!={20}:raise RuntimeError('Initial Kafka map is not 20 partitions per consumer')
                    state.update(reference=reference,hot_partitions=hot);save()
                    (audit/'starting-reference.json').write_text(json.dumps(reference,indent=2)+'\n')
                    config,plan=prepared('kafka_scale6',0,81,reference,hot,aggregate_rate=args.aggregate_rate,hot_count=args.hot_partitions,deadline_ms=deadline_ms)
                    plan.update(action='none',target_consumers=None,prepare_only=True)
                    run_one(config,plan,'verify-native-restart',preparation=True)
                    if not args.preparations_only:
                        # Native rebalance correctness is checked before counting any performance trial.
                        config,plan=prepared('kafka_scale6',0,81,reference,hot,gate=True,aggregate_rate=args.aggregate_rate,hot_count=args.hot_partitions,deadline_ms=deadline_ms)
                        directory,m,checked=run_one(config,plan,'native-scale-technical',technical=True)
                        state['technical'].append(dict(directory=str(directory),validation=checked));save()
                if not args.preparations_only:
                    for arm,number,seed in order[len(state['runs']):]:
                        if shutil.disk_usage(ROOT).free<4*1024**3:raise RuntimeError('Less than 4 GiB free; preserve evidence before continuing')
                        config,plan=prepared(arm,number,seed,reference,hot,aggregate_rate=args.aggregate_rate,hot_count=args.hot_partitions,deadline_ms=deadline_ms)
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
