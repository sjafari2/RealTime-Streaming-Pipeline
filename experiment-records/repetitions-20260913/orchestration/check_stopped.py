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
b=read(A/'block/block-status.json');assert b['restoration']=='verified'
assert b['order']==['none','scale']

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

print('All applications stopped:', len(rows), 'pods')
