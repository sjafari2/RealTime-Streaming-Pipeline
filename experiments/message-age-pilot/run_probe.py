"""Run an isolated queue-age pilot and restore the starting replica count."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--execute',action='store_true')
args=parser.parse_args()
if not args.execute: parser.error('--execute is required for cluster work')
OUT=args.output.resolve(); OUT.mkdir(parents=True,exist_ok=False)
NAMESPACE='kafkastreamingdata'
K=['kubectl','-n',NAMESPACE]
TOKEN='age-probe-20261030-'+str(time.time_ns())[-10:]
REMOTE='/config/'+TOKEN
consumer='consumer-sts-0'
producer='producer-workers-0'


def call(args, timeout=45, data=None):
    p=subprocess.run(K+args,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if p.returncode: raise RuntimeError(p.stderr.decode()[-2500:])
    return p.stdout


def remote(pod,code,data=None,timeout=45):
    container='consumer-container' if pod.startswith('consumer') else 'producer-container'
    return call(['exec','-i',pod,'-c',container,'--','python3','-c',code],timeout,data)


def state(): return json.loads(call(['get','sts','consumer-sts','-o','json']))


def pod_records():
    data=json.loads(call(['get','pods','-l','app=consumer-sts','-o','json']))
    return [{'pod':x['metadata']['name'],'uid':x['metadata']['uid'],'node':x['spec'].get('nodeName'),
             'phase':x['status'].get('phase'),'deleting':x['metadata'].get('deletionTimestamp'),'conditions':x['status'].get('conditions',[]),
             'resources':[c.get('resources') for c in x['spec']['containers']]} for x in data['items']]


original=state()['spec']['replicas']; processes=[]; logs=[]; outcome={'token':TOKEN,'remote':REMOTE}
if original!=3: raise RuntimeError('Expected three stopped consumers before this pilot')
control=json.loads(remote(consumer,'import json; print(json.dumps(json.load(open("/config/run-control.json"))["state"]))'))
if control!='stopped': raise RuntimeError('Managed run is not stopped')
# Prepare only a new directory; no managed source, configuration or old evidence is changed.
rule_path=HERE/'transition_audit.py'
if not rule_path.exists(): rule_path=HERE.parent/'decision-rule-audit/transition_audit.py'
bundle={'kafka_age_probe.py':(HERE/'kafka_age_probe.py').read_text(),
        'transition_audit.py':rule_path.read_text()}
installer='''import json,sys,pathlib
root=pathlib.Path(sys.argv[1]); root.mkdir(exist_ok=False)
for name,text in json.load(sys.stdin).items(): (root/name).write_text(text)
print(root)
'''
call(['exec','-i',consumer,'-c','consumer-container','--','python3','-c',installer,REMOTE],data=json.dumps(bundle).encode())
remote(producer,'from pathlib import Path; assert Path('+repr(REMOTE+'/kafka_age_probe.py')+').exists(); print("shared directory verified")')
outcome['sources']={name:hashlib.sha256(text.encode()).hexdigest() for name,text in bundle.items()}
outcome['original_replicas']=original; outcome['before']=pod_records()
(OUT/'preflight.json').write_text(json.dumps(outcome,indent=2))
try:
    print('Preparing six pods for an isolated assignment probe.',flush=True)
    call(['scale','sts','consumer-sts','--replicas=6'])
    until=time.monotonic()+300
    while time.monotonic()<until:
        pods=pod_records()
        if len(pods)==6 and all(not p['deleting'] and any(c['type']=='Ready' and c['status']=='True' for c in p['conditions']) for p in pods): break
        time.sleep(3)
    else: raise TimeoutError('Six consumer pods did not become ready')
    outcome['prepared']=pods
    from probe_clocks import probe
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool:
        clocks=list(pool.map(probe,[p['pod'] for p in pods]+[producer]))
    outcome['clock_bounds']=clocks
    if any(c['minimum_offset_seconds'] < -.1 or c['maximum_offset_seconds'] > .1 for c in clocks):
        raise RuntimeError('Clock uncertainty exceeds the 100ms pilot limit')
    (OUT/'preflight.json').write_text(json.dumps(outcome,indent=2))
    for index in range(6):
        pod=f'consumer-sts-{index}'; log=(OUT/f'worker-{index}.log').open('wb'); logs.append(log)
        processes.append(subprocess.Popen(K+['exec',pod,'-c','consumer-container','--','python3','-u',
            REMOTE+'/kafka_age_probe.py','worker','--root',REMOTE,'--index',str(index)],stdout=log,stderr=subprocess.STDOUT))
    co=(OUT/'coordinator.log').open('wb'); logs.append(co)
    coordinator=subprocess.Popen(K+['exec',producer,'-c','producer-container','--','python3','-u',
           REMOTE+'/kafka_age_probe.py','coordinator','--root',REMOTE],stdout=co,stderr=subprocess.STDOUT)
    processes.append(coordinator)
    deadline=time.monotonic()+1500; previously=''
    while coordinator.poll() is None:
        if time.monotonic()>deadline: raise TimeoutError('Pilot exceeded 25-minute bound')
        if any(p.poll() not in (None,0) for p in processes[:-1]): raise RuntimeError('A worker failed')
        content=(OUT/'coordinator.log').read_text(errors='replace')
        if content!=previously:
            print(content[len(previously):].strip(),flush=True); previously=content
        time.sleep(2)
    if coordinator.returncode: raise RuntimeError('Coordinator failed: '+(OUT/'coordinator.log').read_text()[-3000:])
    outcome['status']='completed'
except BaseException as exc:
    outcome['status']='failed'; outcome['error']=repr(exc)
    raise
finally:
    try: remote(consumer,'import json,time; open('+repr(REMOTE+'/stop.json')+',"w").write(json.dumps({"time":time.time()}))')
    except Exception as exc: outcome['stop_error']=repr(exc)
    for p in processes:
        if p.poll() is None:
            try: p.wait(timeout=10)
            except subprocess.TimeoutExpired: p.terminate()
    # Retain all logs, including failed preparations, before restoring allocation.
    try:
        archive=call(['exec',consumer,'-c','consumer-container','--','tar','-czf','-','-C','/config',TOKEN],timeout=120)
        (OUT/'raw-evidence.tar.gz').write_bytes(archive)
        outcome['archive_sha256']=hashlib.sha256(archive).hexdigest()
    except Exception as exc: outcome['backup_error']=repr(exc)
    outcome['after']=pod_records()
    try:
        call(['scale','sts','consumer-sts',f'--replicas={original}'])
        outcome['restored_replicas']=state()['spec']['replicas']
    except Exception as exc: outcome['restore_error']=repr(exc)
    for p in processes:
        if p.poll() is None:
            try: p.wait(timeout=5)
            except subprocess.TimeoutExpired: p.terminate()
    for f in logs: f.close()
    (OUT/'execution.json').write_text(json.dumps(outcome,indent=2))
    print(json.dumps({'status':outcome['status'],'restored_replicas':outcome.get('restored_replicas'),
                      'archive':str(OUT/'raw-evidence.tar.gz'),'error':outcome.get('error')}),flush=True)
