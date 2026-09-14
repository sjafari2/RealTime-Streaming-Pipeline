"""Verify a completed block, archive plot data, and build the paired calculations."""
from pathlib import Path
import argparse,json,os,subprocess,sys,time
ROOT=Path('/Users/soheila/Desktop/Thesis-26-27/code')
WORK=Path('/Users/soheila/Documents/ChatGPT/Stream Processing Project')
PYTHON='/tmp/pipeline-review-venv/bin/python'
HELPERS=ROOT/'experiment-records/8020-comparison-20260913/orchestration'
sys.path.insert(0,str(ROOT/'my-shell'))
import run_experiment as r
p=argparse.ArgumentParser();p.add_argument('audit',type=Path);a=p.parse_args();A=a.audit
read=lambda p:json.loads(p.read_text())
b=read(A/'block/block-status.json');assert b['status']=='complete' and b['restoration']=='verified'
assert b['order']==['none','scale'] and [x['action'] for x in b['runs']]==b['order']
assert len(b['attempts'])==6 and all(x['status'] in ('complete','preparation_verified') for x in b['attempts'])
rows=[]
code='''import json,psutil
rows=[]
for p in psutil.process_iter(['pid','cmdline']):
 cmd=p.info['cmdline'] or []
 if any(x.endswith('/producer.py') or x.endswith('/consumer.py') or x in ('producer.py','consumer.py') for x in cmd):rows.append(p.info)
print(json.dumps(rows))'''
for role in r.ROLES:
 for pod in r.pods(role):
  apps=json.loads(r.remote(role,pod,code));rows.append(dict(role=role,pod=pod,application_processes=apps))
assert not any(x['application_processes'] for x in rows)
(A/'final-runtime-check.json').write_text(json.dumps(dict(checked_epoch=time.time(),all_applications_stopped=True,pods=rows),indent=2)+'\n')
def call(args):
 print('Running',Path(args[0]).name,flush=True)
 subprocess.run([PYTHON,*map(str,args)],check=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',MPLCONFIGDIR='/tmp/pipeline-matplotlib'))
for item in b['attempts']:
 call([HELPERS/'verify_evidence.py',ROOT/'results'/item['run_id']])
index=dict(family=b['workload']+'-repeat',status='complete',expected_pair_count=1,
           pairs=[dict(pair=b['workload']+'-s71-repeat',**{x['action']:x['run_id'] for x in b['runs']})])
(A/'comparison-index.json').write_text(json.dumps(index,indent=2)+'\n')
call([ROOT/'python-scripts/compare_runs.py',A/'comparison-index.json','--output',A/'comparison'])
for item in b['runs']:
 out=A/'plots'/item['action']
 call([HELPERS/'export_plot_data.py',ROOT/'results'/item['run_id'],'--output',out])
 call([HELPERS/'draw_run.py',out])
 out=A/'transitions'/item['action'];out.mkdir(parents=True,exist_ok=True)
 call([ROOT/'experiment-records/8020-comparison-20260913/transitions/audit_transitions.py',ROOT/'results'/item['run_id'],out/'transition-audit.json'])
print('Block evidence, plots and transition audits collected.',flush=True)
