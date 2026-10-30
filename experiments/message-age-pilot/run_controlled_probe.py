"""Run the controlled assignment diagnostic with durable remote status files."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import time


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    if not args.execute: parser.error('--execute is required')
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    here=Path(__file__).resolve().parent
    token='age-controlled-20261030-'+str(time.time_ns())[-10:]
    root='/config/'+token
    command=['kubectl','--context','nautilus','-n','kafkastreamingdata']
    def call(rest,data=None,timeout=45):
        p=subprocess.run(command+rest,input=data,capture_output=True,timeout=timeout)
        if p.returncode: raise RuntimeError(p.stderr.decode()[-2000:])
        return p.stdout
    def remote(pod,code,data=None,timeout=45):
        container='producer-container' if pod.startswith('producer') else 'consumer-container'
        return call(['exec','-i',pod,'-c',container,'--','python3','-c',code],data,timeout)
    def replicas(): return json.loads(call(['get','sts','consumer-sts','-o','json']))['spec']['replicas']
    def pods():
        data=json.loads(call(['get','pods','-l','app=consumer-sts','-o','json']))
        return [dict(pod=p['metadata']['name'],uid=p['metadata']['uid'],node=p['spec'].get('nodeName'),
            deleting=p['metadata'].get('deletionTimestamp'),conditions=p['status'].get('conditions',[]),
            resources=[c.get('resources') for c in p['spec']['containers']],
            restart_counts=[c['restartCount'] for c in p['status'].get('containerStatuses',[])]) for p in data['items']]
    def status():
        code='''import pathlib,json
r=pathlib.Path(ROOT)
names=['coordinator']+['worker-'+str(i) for i in range(6)]
s={n:json.loads((r/(n+'-process.json')).read_text()) if (r/(n+'-process.json')).exists() else None for n in names}
print(json.dumps({'processes':s,'summaries':json.loads((r/'summary.json').read_text()) if (r/'summary.json').exists() else []}))
'''.replace('ROOT',repr(root))
        return json.loads(remote('consumer-sts-0',code))
    original=replicas()
    if original!=3: raise RuntimeError('Expected three consumers before preparation')
    managed=json.loads(remote('consumer-sts-0','import json; print(json.dumps(json.load(open("/config/run-control.json"))["state"]))'))
    if managed!='stopped': raise RuntimeError('Managed run is active')
    names=['kafka_age_probe.py','controlled_age_probe.py','capacity_age_ablation.py','probe_process.py']
    bundle={name:(here/name).read_text() for name in names}
    install='import sys,json,pathlib; r=pathlib.Path('+repr(root)+'); r.mkdir(exist_ok=False); [(r/n).write_text(t) for n,t in json.load(sys.stdin).items()]'
    remote('consumer-sts-0',install,json.dumps(bundle).encode())
    execution=dict(token=token,remote=root,original_replicas=original,before=pods(),
        sources={n:hashlib.sha256(t.encode()).hexdigest() for n,t in bundle.items()})
    (output/'preflight.json').write_text(json.dumps(execution,indent=2))
    try:
        call(['scale','sts','consumer-sts','--replicas=6'])
        deadline=time.monotonic()+300
        while time.monotonic()<deadline:
            prepared=pods()
            if len(prepared)==6 and all(not p['deleting'] and any(c['type']=='Ready' and c['status']=='True' for c in p['conditions']) for p in prepared):break
            time.sleep(3)
        else: raise TimeoutError('Consumers did not become ready')
        execution['prepared']=prepared
        from probe_clocks import probe
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool:
            execution['clock_bounds']=list(pool.map(probe,[p['pod'] for p in prepared]+['producer-workers-0']))
        if any(c['minimum_offset_seconds']<-.1 or c['maximum_offset_seconds']>.1 for c in execution['clock_bounds']):
            raise RuntimeError('Clock bounds exceed the preparation limit')
        (output/'preflight.json').write_text(json.dumps(execution,indent=2))
        for i in range(7):
            name='coordinator' if i==6 else 'worker-'+str(i)
            pod='producer-workers-0' if i==6 else 'consumer-sts-'+str(i)
            script='controlled_age_probe.py' if i==6 else 'kafka_age_probe.py'
            child=['python3','-u',root+'/'+script,'coordinator' if i==6 else 'worker','--root',root]
            if i!=6: child+=['--index',str(i)]
            launch=['python3',root+'/probe_process.py','--root',root,'--name',name,'--']+child
            remote(pod,'import subprocess; p=subprocess.Popen('+repr(launch)+',stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True); print(p.pid)')
        print('Remote workers started; status and logs persist if the monitoring connection closes.',flush=True)
        deadline=time.monotonic()+1500; seen=set(); misses=0
        while time.monotonic()<deadline:
            try: saved=status();misses=0
            except Exception:
                misses+=1
                if misses>=3:raise
                time.sleep(3);continue
            (output/'progress.json').write_text(json.dumps(saved,indent=2))
            for row in saved['summaries']:
                if row['id'] not in seen:
                    print(json.dumps(row),flush=True);seen.add(row['id'])
            processes=saved['processes']; co=processes['coordinator']
            if co and co['state']!='running':
                if co['state']!='completed':raise RuntimeError('Remote coordinator failed; inspect retained coordinator.log')
                execution['status']='completed';break
            if any(s and s['state']=='failed' for n,s in processes.items() if n!='coordinator'):
                raise RuntimeError('A remote worker failed')
            time.sleep(10)
        else: raise TimeoutError('Controlled probe exceeded 25 minutes')
    except BaseException as exc:
        execution.update(status='failed',error=repr(exc));raise
    finally:
        try:
            remote('consumer-sts-0','import json,time; open('+repr(root+'/stop.json')+',"w").write(json.dumps({"time":time.time()}))')
            for _ in range(12):
                saved=status()
                if all(not s or s['state']!='running' for s in saved['processes'].values()):break
                time.sleep(2)
            else:
                # Only terminate processes whose command contains this run's
                # unique script path; never signal a managed pipeline process.
                for pod in ['producer-workers-0']+['consumer-sts-'+str(i) for i in range(6)]:
                    code='''import psutil,os
root=ROOT
targets=[]
for p in psutil.process_iter(['pid','cmdline']):
 if p.pid!=os.getpid() and any(x.startswith(root+'/') and x.endswith('.py') for x in (p.info['cmdline'] or [])): targets.append(p)
for p in targets: p.terminate()
_,alive=psutil.wait_procs(targets,timeout=5)
for p in alive:p.kill()
'''.replace('ROOT',repr(root))
                    remote(pod,code)
                execution['forced_probe_cleanup']=True
            archive=call(['exec','consumer-sts-0','-c','consumer-container','--','tar','-czf','-','-C','/config',token],timeout=120)
            (output/'raw-evidence.tar.gz').write_bytes(archive)
            execution['archive_sha256']=hashlib.sha256(archive).hexdigest()
        except Exception as exc:execution['backup_or_stop_error']=repr(exc)
        try: execution['after']=pods()
        except Exception as exc: execution['after_snapshot_error']=repr(exc)
        try:
            call(['scale','sts','consumer-sts','--replicas='+str(original)])
            execution['restored_replicas']=replicas()
        except Exception as exc:execution['restore_error']=repr(exc)
        (output/'execution.json').write_text(json.dumps(execution,indent=2))
        print(json.dumps({k:execution.get(k) for k in ['status','token','restored_replicas','error','backup_or_stop_error']}),flush=True)


if __name__=='__main__':main()
