"""Copy edited application code once per shared PVC, after checking all pods are idle."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from run_experiment import ROOT, ROLES, pods, remote, read_control

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--role', choices=['producer', 'consumer', 'all'], default='all')
args = parser.parse_args()
control = read_control()
if control and control['state'] in ('preparing', 'running'):
    raise SystemExit('Run save-run.sh stop before copying code.')
roles = list(ROLES) if args.role == 'all' else [args.role]
for role in roles:
    names = pods(role)
    if not names:
        raise SystemExit('No pods for ' + role)
    for pod in names:
        code = f'''import os,json
from pathlib import Path
running=[]
for p in Path('/proc').iterdir():
    try:
        argv=(p/'cmdline').read_bytes().split(b'\\0')
        if len(argv)>1 and Path(os.fsdecode(argv[1])).name=={(role + '.py')!r}: running.append(p.name)
    except OSError: pass
print(json.dumps(running))
'''
        if json.loads(remote(role, pod, code)):
            raise SystemExit(f'{pod} still runs {role}.py; stop it before changing shared files')
    files = [ROOT / 'src' / role / (role + '.py'), ROOT / 'src' / role / 'run.sh',
             ROOT / 'src/common/pipeline_runtime.py', ROOT / 'src/common/launch.py']
    if role == 'consumer':
        files += [ROOT / 'src/consumer/supervise.py', ROOT / 'src/consumer/create_topics.sh']
    for source in files:
        destination = ROLES[role][1] + '/' + source.name
        code = f'''import os,sys,tempfile
from pathlib import Path
p=Path({destination!r}); f=tempfile.NamedTemporaryFile(dir=p.parent,delete=False)
f.write(sys.stdin.buffer.read()); f.flush(); os.fsync(f.fileno()); f.close()
os.chmod(f.name,0o755 if p.suffix=='.sh' else 0o644); os.replace(f.name,p)
'''
        remote(role, names[0], code, source.read_bytes())
        expected = hashlib.sha256(source.read_bytes()).hexdigest()
        for pod in names:
            actual = remote(role, pod, f'import hashlib; print(hashlib.sha256(open({destination!r},"rb").read()).hexdigest())').decode().strip()
            if actual != expected:
                raise SystemExit(f'Shared-volume verification failed for {pod}: {destination}')
        print('[SYNC]', source.name, 'verified in', len(names), role, 'pods')
