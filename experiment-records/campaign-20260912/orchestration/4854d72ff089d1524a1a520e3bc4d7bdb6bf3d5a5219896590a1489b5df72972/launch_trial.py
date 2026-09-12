"""Run one explicitly selected campaign trial through the existing coordinator."""
import argparse,hashlib,json,os,shutil,subprocess,sys,time
from pathlib import Path
import yaml
ROOT=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'my-shell'));import run_experiment as r
p=argparse.ArgumentParser();p.add_argument('--label',required=True);p.add_argument('--config',type=Path,required=True);p.add_argument('--initial-consumers',type=int,required=True);p.add_argument('--plan',type=Path);a=p.parse_args()
T=A/a.label;T.mkdir(exist_ok=True);assert not (T/'launch.json').exists(),'Use a new label for each trial'
raw=a.config.read_bytes();config=yaml.safe_load(raw)['data'];plan=json.loads(a.plan.read_text()) if a.plan else None
assert float(config['EXP_DURATION_SEC'])<=600
projection=float(config['TARGET_RATE'])*int(config['PRODUCER_POD_COUNT'])*float(config['EXP_DURATION_SEC'])*850
assert shutil.disk_usage(ROOT).free>=4*projection+8*2**30,'Insufficient conservative local workspace reserve'
with r.command_lock():
 assert r.kubectl('config','current-context').decode().strip()=='nautilus'
 current=r.read_control()
 if current and current['state'] in ('running','preparing'):
  previous=ROOT/'results'/current['run_id']/'runner-status.json'
  assert previous.exists() and json.loads(previous.read_text())['status']=='complete','Preserve an unfinished/unvalidated previous run'
  assert time.time()>current['drain_end_epoch']+15
 r.stop()
 before=r.shared_read(r.CONFIG);(T/'previous-shared-config.yaml').write_bytes(before)
 r.shared_write('/config/backups/'+a.label+'-before.yaml',before)
 r.validate_config(config);r.validate_intervention(plan,config)
 for role,minimum in [('producer',2),('consumer',5)]:
  free=int(r.remote(role,r.pods(role)[0],f'import shutil;print(shutil.disk_usage({r.ROLES[role][1]!r}).free)').decode())
  assert free>=minimum*2**30,(role,'Insufficient PVC space')
 r.shared_write(r.CONFIG,raw)
 if not plan:r.set_consumer_baseline(a.initial_consumers,float(config.get('READINESS_TIMEOUT_SECONDS',180)))
 record=dict(pid=os.getpid(),launch_epoch=time.time(),label=a.label,config_path=str(a.config.resolve()),config_sha256=hashlib.sha256(raw).hexdigest(),git_revision=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip(),git_status=subprocess.check_output(['git','-C',str(ROOT),'status','--porcelain'],text=True),plan=plan,status='starting',experiment_id=config['EXP_ID'])
 (T/'launch.json').write_text(json.dumps(record,indent=2)+'\n');(T/'reviewed-config.yaml').write_bytes(raw)
 try:
  r.preflight()
  with r.prometheus_connection():result=r.complete_run(plan)
  record.update(status='complete',result_directory=str(result))
 except BaseException as exc:
  record.update(status='failed_or_interrupted',error=str(exc) or type(exc).__name__);raise
 finally:
  record['finished_epoch']=time.time();(T/'launch.json').write_text(json.dumps(record,indent=2)+'\n')
