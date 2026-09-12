"""Preserve calibration monitoring and interrupt only its owning runner on a limit."""
import argparse,json,math,os,signal,sys,time,urllib.parse,urllib.request
from pathlib import Path
import psutil
A=Path(__file__).resolve().parent
sys.path.insert(0,'/Users/soheila/Desktop/Thesis-26-27/code/my-shell')
import run_experiment as r
sys.path.insert(0,str(Path(r.ROOT)/'python-scripts'))
from lag_freshness import observation_validity
parser=argparse.ArgumentParser();parser.add_argument('--label',required=True);parser.add_argument('--monitoring-transition-seconds',type=float,default=0);args=parser.parse_args()
A=A/args.label
for _ in range(180):
    if (A/'launch.json').exists(): break
    time.sleep(2)
else: raise RuntimeError('Trial launcher did not become ready')
launch=json.loads((A/'launch.json').read_text());owner=launch['pid']
command=psutil.Process(owner).cmdline()
assert 'review/preliminary-campaign-20260912/launch_trial.py' in command and args.label in command
for _ in range(60):
    c=r.read_control()
    if c and c.get('state')=='running' and c.get('config',{}).get('EXP_ID')==launch['experiment_id']: break
    time.sleep(3)
else: raise RuntimeError('The expected pressure run did not start')
(A/'control.json').write_text(json.dumps(c,indent=2)+'\n')
print('Monitoring',c['run_id'],'runner',owner,flush=True)
invalid_since=high_since=None;resource_at=0;aborted=False
names=['consumer_partition_owned','consumer_lag_valid','consumer_lag_observed_timestamp_seconds','consumer_lag_observation_age_seconds','consumer_processing_backlog','consumer_lag','consumer_position_offset','consumer_high_offset','consumer_evidence_dropped_total','producer_evidence_dropped_total','producer_delivery_err_total','consumer_processing_failures_total','consumer_commit_failures_total','consumer_cpu_percent','producer_cpu_percent']
expected={(f"{c['config']['TOPIC_TITLE']}_{t}",str(p)) for t in range(int(c['config']['TOPIC_COUNT'])) for p in range(int(c['config']['NUM_PARTITIONS']))}
with r.prometheus_connection():
    base=os.environ['PROM_URL']
    while time.time()<c['drain_end_epoch']+15:
        now=time.time(); sample=now-1;error=None;entries={};pods={};bad=0
        query='{__name__=~'+json.dumps('|'.join(names))+',run_id='+json.dumps(c['run_id'])+'}'
        query = r.add_lag_sample_timestamps(query, c['run_id'])
        try:
            params=urllib.parse.urlencode(dict(query=query,time=sample))
            with urllib.request.urlopen(base+'/api/v1/query?'+params,timeout=5) as response: data=json.load(response)
            assert data.get('status')=='success'
            for s in data['data']['result']:
                m=s['metric'];name=m['__name__'];v=float(s['value'][1])
                if 'partition' in m:
                    entries.setdefault((m.get('topic'),m.get('partition'),m['pod'],m['incarnation']),{})[name]=v
                else:
                    pods.setdefault(m['pod'],{})[name]=v if math.isfinite(v) else None
                    if name.endswith(('evidence_dropped_total','delivery_err_total','processing_failures_total')) and v>0: bad+=v
            selected={};duplicate=False
            for (topic,p,pod,inc),m in entries.items():
                if m.get('consumer_partition_owned')==1:
                    if (topic,p) in selected: duplicate=True
                    selected[(topic,p)]=(pod,m)
            valid=set(selected)==expected and not duplicate
            total=0;busy=0;per_pod={};age_max=0;invalid_partitions=[]
            for _,(pod,m) in selected.items():
                back=m.get('consumer_processing_backlog',math.nan)
                good,age=observation_validity(m,sample,float(c['config'].get('LAG_FRESHNESS_SECONDS',10)))
                if not good: invalid_partitions.append(dict(pod=pod,metrics={k:v if math.isfinite(v) else None for k,v in m.items()}))
                valid=valid and good
                if good: total+=back;busy+=back>=10;per_pod[pod]=per_pod.get(pod,0)+back;age_max=max(age_max,age)
            row=dict(timestamp=now,query_timestamp=sample,seconds_from_start=now-c['start_epoch'],valid=valid,invalid_partitions=invalid_partitions,freshness_clock="monotonic_scrape_v2",owned_partitions=len(selected),processing_backlog=total if valid else None,partitions_with_backlog_at_least_10=busy if valid else None,per_consumer_backlog=per_pod if valid else {},maximum_observation_age=age_max if valid else None,pod_metrics=pods,evidence_or_send_errors=bad)
        except Exception as exc:
            valid=False;row=dict(timestamp=now,query_timestamp=sample,valid=False,error=str(exc))
        if now<c['producer_end_epoch']:
            if valid: invalid_since=None
            elif now>=c['evaluation_start_epoch']:
                action=c.get('intervention',{})
                in_transition=action.get('action')=='scale' and action['at_epoch']<=now<action['at_epoch']+args.monitoring_transition_seconds
                if in_transition: invalid_since=None
                else: invalid_since=invalid_since or now
            if valid and total>600000: high_since=high_since or now
            else: high_since=None
            reason='Evidence loss or delivery error' if bad else 'Invalid monitoring for ten seconds' if invalid_since and now-invalid_since>=10 else 'Processing backlog above 600000 for ten seconds' if high_since and now-high_since>=10 else None
            if reason and not aborted:
                current=r.read_control()
                if not current or current.get('run_id')!=c['run_id'] or current.get('state')!='running' or current.get('intervention_cancelled_epoch'):
                    row['stop_already_requested']=True;aborted=True
                    with (A/'live-monitor.jsonl').open('a') as stream: stream.write(json.dumps(row,allow_nan=False)+'\n')
                    print('Preserving the existing stop; no second interrupt.',flush=True)
                    break
                row['stop_reason']=reason;aborted=True
                (A/'guard-stop.json').write_text(json.dumps(dict(run_id=c['run_id'],timestamp=now,reason=reason,status='threshold_limited'),indent=2)+'\n')
                os.kill(owner,signal.SIGINT)
                print('Requested managed stop:',reason,flush=True)
        with (A/'live-monitor.jsonl').open('a') as stream: stream.write(json.dumps(row,allow_nan=False)+'\n')
        print(json.dumps({k:row.get(k) for k in ('seconds_from_start','valid','processing_backlog','partitions_with_backlog_at_least_10','error')}),flush=True)
        if now>=resource_at:
            try:
                resource=json.loads(r.kubectl('get','--raw','/apis/metrics.k8s.io/v1beta1/namespaces/kafkastreamingdata/pods',timeout=10))
                with (A/'container-resources.jsonl').open('a') as stream: stream.write(json.dumps(dict(timestamp=time.time(),response=resource))+'\n')
            except Exception as exc: print('Resource observation missing:',str(exc),flush=True)
            resource_at=now+30
        time.sleep(max(.1,5-(time.time()-now)))
