"""Measure the existing synthetic digest task while experiment applications are stopped."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json,sys,time
R=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent
sys.path.insert(0,str(R/'my-shell'));import run_experiment as r
CODE=r'''
import hashlib,importlib.metadata,json,platform,statistics,time
from pathlib import Path
running=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   argv=(p/'cmdline').read_bytes().split(b'\0')
   if len(argv)>1 and Path(argv[1].decode()).name=='consumer.py':running.append(p.name)
  except (OSError,UnicodeError):pass
assert not running, 'Do not benchmark during an experiment'
rows=[]
for n in (1000,2000,4000,8000):
 samples=[]
 for repeat in range(3):
  before=time.perf_counter();cpu=time.thread_time()
  for message in range(50):
   result=b'x'*100
   for _ in range(n):result=hashlib.sha256(result).digest()
  samples.append(dict(wall_seconds_per_message=(time.perf_counter()-before)/50,thread_cpu_seconds_per_message=(time.thread_time()-cpu)/50))
 rows.append(dict(iterations=n,samples=samples,median_wall_seconds=statistics.median(x['wall_seconds_per_message'] for x in samples),median_thread_cpu_seconds=statistics.median(x['thread_cpu_seconds_per_message'] for x in samples)))
print(json.dumps(dict(measured_epoch=time.time(),python=platform.python_version(),packages={name:importlib.metadata.version(name) for name in ('confluent-kafka','prometheus-client','psutil','PyYAML')},measurements=rows)))
'''
with r.command_lock():
 c=r.read_control();assert c['run_id']=='run-20260912-043547' and time.time()>c['drain_end_epoch']+15
 assert json.loads((R/'results'/c['run_id']/'runner-status.json').read_text())['status']=='complete'
 r.stop();before=time.time();record=dict(started_epoch=before,scope='Stopped-application CPU task microbenchmark; excluded from experiment outcome and resource-efficiency comparisons',entry_consumer_count=len(r.pods('consumer')))
 try:
  r.set_consumer_baseline(6,180);record['six_ready_observed_epoch']=time.time();r.preflight()
  names=r.pods('consumer')
  def bench(pod):return dict(pod=pod,**json.loads(r.remote('consumer',pod,CODE,timeout=30)))
  with ThreadPoolExecutor(max_workers=3) as pool:record['pods']=list(pool.map(bench,names))
  record['pod_deployment']=json.loads(r.kubectl('get','pods','-l','app=consumer-sts','-o','json'))
  versions={json.dumps(dict(python=x['python'],packages=x['packages']),sort_keys=True) for x in record['pods']}
  record['uniform_consumer_python_and_packages']=len(versions)==1
  for row in record['pods']:print(row['pod'],[(x['iterations'],round(1000*x['median_wall_seconds'],4)) for x in row['measurements']],row['packages'],flush=True)
  assert len(versions)==1,'New and existing consumer package versions differ; resolve before comparison'
 finally:
  r.set_consumer_baseline(record['entry_consumer_count'],180)
  record['finished_epoch']=time.time();(A/'cpu-task-calibration.json').write_text(json.dumps(record,indent=2)+'\n')
print('Saved isolated task calibration.',flush=True)
