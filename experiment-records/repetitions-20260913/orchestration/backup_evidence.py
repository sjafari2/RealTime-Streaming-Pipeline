from pathlib import Path
import tarfile,hashlib,json
import sys
A=Path(sys.argv[1]).resolve();R=Path('/Users/soheila/Desktop/Thesis-26-27/code/results')
b=json.loads((A/'block/block-status.json').read_text());names=[x['run_id'] for x in b['attempts'] if x.get('run_id')]
archive=A/'completed-block-evidence.tar.gz';assert not archive.exists()
def digest(f):
 h=hashlib.sha256()
 for z in iter(lambda:f.read(1024*1024),b''):h.update(z)
 return h.hexdigest()
expected={}
with tarfile.open(archive,'w:gz',compresslevel=1) as t:
 for rid in names:
  for p in sorted((R/rid).rglob('*')):
   if p.is_file():
    name=str(p.relative_to(R))
    with p.open('rb') as f:expected[name]=digest(f)
    t.add(p,arcname=name,recursive=False)
seen=set()
with tarfile.open(archive,'r:gz') as t:
 for member in t:
  assert member.isfile()
  with t.extractfile(member) as f:assert digest(f)==expected[member.name],member.name
  seen.add(member.name)
assert seen==set(expected)
with archive.open('rb') as f:sha=digest(f)
(A/'evidence-backup-manifest.json').write_text(json.dumps({'archive':str(archive),'sha256':sha,'bytes':archive.stat().st_size,'files':expected,'verified_round_trip':True,'off_machine_backup':False},indent=2)+'\n')
print('Verified archive:',len(seen),'files;',archive.stat().st_size,'bytes',flush=True)
