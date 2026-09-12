"""Launch one reviewed calibration through the existing managed runner."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
ROOT=Path('/Users/soheila/Desktop/Thesis-26-27/code')
A=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'my-shell'))
import run_experiment as r
raw=(ROOT/'experiments/capacity-calibration/balanced-pressure.yaml').read_bytes()
previous=json.loads((A/'control-before.json').read_text())
with r.command_lock():
    assert r.kubectl('config','current-context').decode().strip()=='nautilus'
    control=r.read_control()
    assert control['run_id']==previous['run_id'] and control['state']=='stopped'
    assert r.shared_read(r.CONFIG)==(A/'shared-config-before.yaml').read_bytes()
    r.shared_write('/config/backups/preliminary-campaign-20260912-before.yaml',(A/'shared-config-before.yaml').read_bytes())
    r.shared_write(r.CONFIG,raw)
    expected=hashlib.sha256(raw).hexdigest()
    rows=[]
    for role in r.ROLES:
        for pod in r.pods(role):
            actual=r.remote(role,pod,"import hashlib;from pathlib import Path;print(hashlib.sha256(Path('/config/pipeline-configmap.yaml').read_bytes()).hexdigest())").decode().strip()
            assert actual==expected,(pod,actual,expected)
            rows.append(dict(role=role,pod=pod,config_sha256=actual))
    (A/'applied-config-hashes.json').write_text(json.dumps(rows,indent=2)+'\n')
    provenance=dict(launch_epoch=time.time(),git_revision=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip(),git_status=subprocess.check_output(['git','-C',str(ROOT),'status','--porcelain'],text=True),config_sha256=expected,monitoring='Original fixed six-consumer targets at 5-second scrape interval; broker timeout unresolved; see admission record',stage='balanced-12000-no-app-work')
    (A/'pressure-launch.json').write_text(json.dumps(provenance,indent=2)+'\n')
    r.preflight()
    with r.prometheus_connection():
        result=r.complete_run()
    provenance.update(status='complete',result_directory=str(result),finished_epoch=time.time())
    (A/'pressure-launch.json').write_text(json.dumps(provenance,indent=2)+'\n')
