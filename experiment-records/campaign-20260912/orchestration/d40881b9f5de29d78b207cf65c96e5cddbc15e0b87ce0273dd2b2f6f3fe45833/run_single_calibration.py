"""Run the predeclared dedicated-partition check after the reviewed core campaign."""
import hashlib,json,os,signal,subprocess,sys,time
from pathlib import Path
A=Path(__file__).resolve().parent;W=A.parents[1];R=Path('/Users/soheila/Desktop/Thesis-26-27/code');E=R/'experiments/preliminary-campaign-20260912'
sys.path.insert(0,str(R/'my-shell'));import run_experiment as r
core=json.loads((A/'sequence-status-v4.json').read_text());assert core['status']=='complete' and len(core['trials'])==8 and all(x['status']=='complete' for x in core['trials'])
for family in ['sustained','short']:
 d=A/'comparisons'/family;index=json.loads((d/'index.json').read_text());summary=json.loads((d/'comparison-summary.json').read_text())
 assert index['status']=='complete_predeclared_family' and len(summary['pairs'])==2
 assert all(not p['compatibility_failures'] and all(z['evidence_valid'] for z in p['runs'].values()) for p in summary['pairs'])
plan=json.loads((E/'single-partition-capacity-plan.json').read_text());label=plan['label']
assert not (A/label/'launch.json').exists(),'Retain any existing attempt; inspect it before choosing a new label'
assert time.time()+180+120+900<1789246384-3600
for kind in ['config','plan']:assert hashlib.sha256((E/plan[kind]).read_bytes()).hexdigest()==plan[kind+'_sha256']
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',MPLCONFIGDIR='/tmp/pipeline-matplotlib');record=dict(status='starting',label=label,started_epoch=time.time());state=A/'single-partition-status.json';assert not state.exists()
def save():state.write_text(json.dumps(record,indent=2)+'\n')
def execute(command):
 with (A/(label+'-sequence.log')).open('a') as out:subprocess.run(command,cwd=W,env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
save();print('Starting',label,flush=True)
try:
 with (A/(label+'-runner.log')).open('w') as out,(A/(label+'-guard.log')).open('w') as monitor:
  runner=subprocess.Popen([sys.executable,'-u','review/preliminary-campaign-20260912/launch_trial.py','--label',label,'--config',str(E/plan['config']),'--initial-consumers','1','--plan',str(E/plan['plan'])],cwd=W,env=env,stdout=out,stderr=subprocess.STDOUT)
  guard=subprocess.Popen([sys.executable,'-u','review/preliminary-campaign-20260912/guard_trial.py','--label',label],cwd=W,env=env,stdout=monitor,stderr=subprocess.STDOUT)
  guard_failed=False
  while runner.poll() is None:
   if guard.poll() not in (None,0) and not guard_failed:
    guard_failed=True;current=r.read_control()
    if current and current.get('config',{}).get('EXP_ID')=='single-partition-capacity' and not current.get('intervention_cancelled_epoch') and (current['state']=='preparing' or current['state']=='running' and time.time()<current['drain_end_epoch']):runner.send_signal(signal.SIGINT)
   time.sleep(2)
  try:guard.wait(timeout=15)
  except subprocess.TimeoutExpired:guard.terminate();guard.wait(timeout=10)
  assert runner.returncode==0,'Calibration failed; preserve and inspect its available evidence'
 launch=json.loads((A/label/'launch.json').read_text());D=Path(launch['result_directory']);record.update(status='preserving_evidence',run_id=D.name);save()
 for script in ['verify_evidence.py','export_plot_data.py']:execute([sys.executable,str(A/script),str(D)])
 execute([sys.executable,str(A/'draw_run.py'),str(D/'plots')])
 execute([sys.executable,str(R/'python-scripts/compress_evidence.py'),str(D)])
 execute([sys.executable,str(A/'package_record.py'),D.name,'--label',label,'--qualification','Dedicated one-partition/one-consumer capacity calibration at 1200 messages/s and 2000 SHA-256 iterations/message. Interpret only after the separately recorded qualification checks; this is one observed node and interval, not a universal service-capacity constant.'])
 execute(['git','-C',str(R),'add','--','experiment-records/'+D.name]);execute(['git','-C',str(R),'diff','--cached','--check']);execute(['git','-C',str(R),'commit','--quiet','-m','Preserve dedicated-partition calibration evidence and figures'])
 assert not guard_failed,'Guard failed; inspect the retained calibration before using it'
 execute([sys.executable,str(A/'qualify_single_partition.py'),D.name])
 qualification=json.loads((A/'single-partition-qualification.json').read_text());record.update(status='complete',qualified=qualification['qualified'])
except BaseException as exc:record.update(status='failed',error=str(exc) or type(exc).__name__);raise
finally:record['finished_epoch']=time.time();save();print(json.dumps(record),flush=True)
