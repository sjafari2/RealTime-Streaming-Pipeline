"""Check archived probe files against the retained cluster copies."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile


def verify(directory):
    execution=json.loads((directory/'execution.json').read_text())
    archive=directory/'raw-evidence.tar.gz'
    archive_hash=hashlib.sha256(archive.read_bytes()).hexdigest()
    assert archive_hash==execution['archive_sha256']
    expected={}
    with tarfile.open(archive) as tf:
        for item in tf.getmembers():
            if item.isfile():
                expected[item.name.split('/',1)[1]]=hashlib.sha256(tf.extractfile(item).read()).hexdigest()
    code='''import pathlib,hashlib,json
root=pathlib.Path(ROOT)
print(json.dumps({str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}))
'''.replace('ROOT',repr(execution['remote']))
    response=subprocess.run(['kubectl','--context','nautilus','-n','kafkastreamingdata',
        'exec','consumer-sts-0','-c','consumer-container','--','python3','-c',code],
        capture_output=True,text=True,check=True,timeout=120)
    remote=json.loads(response.stdout)
    missing=[name for name in expected if name not in remote]
    changed=[name for name in expected if name in remote and expected[name]!=remote[name]]
    result=dict(token=execution['token'],archive_sha256=archive_hash,
        files_verified=len(expected)-len(missing)-len(changed),missing=missing,changed=changed,
        additional_remote_files=sorted(set(remote)-set(expected)),
        verified=not missing and not changed)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories',type=Path,nargs='+')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();results=[verify(d) for d in args.directories]
    args.output.write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps(results,indent=2))
    if not all(r['verified'] for r in results):raise SystemExit(1)
