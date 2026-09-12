"""Match collected evidence to the same closed run retained on the shared PVCs."""
import argparse,gzip,hashlib,json,sys,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
ROOT=Path('/Users/soheila/Desktop/Thesis-26-27/code');sys.path.insert(0,str(ROOT/'my-shell'))
import run_experiment as r
p=argparse.ArgumentParser();p.add_argument('run_directory',type=Path);p.add_argument('--output',type=Path);a=p.parse_args()
D=a.run_directory;c=json.loads((D/'manifest.json').read_text())

def digest(stream):
 h=hashlib.sha256()
 for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
 return h.hexdigest()

def verify(role):
 source=str(c['config'].get(role.upper()+'_EVIDENCE_DIR',r.ROLES[role][1]+'/evidence'))+'/'+c['run_id']
 code='''import hashlib,json\nfrom pathlib import Path\nroot=Path(SOURCE)\nrows=[]\nfor p in sorted(root.rglob('*')):\n if p.is_file():\n  h=hashlib.sha256()\n  with p.open('rb') as stream:\n   for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)\n  rows.append(dict(path=str(p.relative_to(root)),sha256=h.hexdigest(),bytes=p.stat().st_size))\nprint(json.dumps(rows))'''.replace('SOURCE',repr(source))
 pod=r.pods(role)[0];remote=json.loads(r.remote(role,pod,code,timeout=180))
 rows=[]
 for x in remote:
  local=D/role/x['path'];compressed=False
  if not local.exists() and local.with_name(local.name+'.gz').exists():local=local.with_name(local.name+'.gz');compressed=True
  with gzip.open(local,'rb') if compressed else local.open('rb') as stream:actual=digest(stream)
  assert actual==x['sha256'],str(local)
  rows.append(dict(role=role,source_path=x['path'],local_path=str(local.relative_to(D)),original_sha256=actual,original_bytes=x['bytes'],matches_source=True))
 return dict(role=role,read_from_pod=pod,files=rows)
with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(verify,r.ROLES))
report=dict(run_id=c['run_id'],verified_epoch=time.time(),all_files_match=True,file_count=sum(len(x['files']) for x in results),roles=results)
out=a.output or D/'evidence-verification.json';out.write_text(json.dumps(report,indent=2)+'\n');print('Matched',report['file_count'],'files for',c['run_id'],flush=True)
