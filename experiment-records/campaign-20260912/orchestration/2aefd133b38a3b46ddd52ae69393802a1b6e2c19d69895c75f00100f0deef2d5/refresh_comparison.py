"""Analyze completed matched pairs only; a partial family stays explicitly partial."""
import argparse,hashlib,json,os,subprocess,sys
from pathlib import Path
A=Path(__file__).resolve().parent;R=Path('/Users/soheila/Desktop/Thesis-26-27/code')
p=argparse.ArgumentParser();p.add_argument('family');p.add_argument('--ledger',default='sequence-status-v4.json');a=p.parse_args()
path=A/a.ledger;ledger=json.loads(path.read_text());protocol=json.loads(Path(ledger['protocol']).read_text())
expected={r['pair'] for r in protocol['trials'] if r['family']==a.family};grouped={}
for row in ledger['trials']:
 if row['family']==a.family and row['status']=='complete':grouped.setdefault(row['pair'],{})[row['action']]=row['run_id']
pairs=[dict(pair=key,**runs) for key,runs in sorted(grouped.items()) if set(runs)=={'none','scale'}]
assert pairs,'No completed matched pair yet'
status='complete_predeclared_family' if len(pairs)==len(expected) else f'partial_{len(pairs)}_of_{len(expected)}_predeclared_pairs'
D=A/'comparisons'/a.family;D.mkdir(parents=True,exist_ok=True)
index=dict(status=status,family=a.family,protocol=str(Path(ledger['protocol']).name),protocol_sha256=hashlib.sha256(Path(ledger['protocol']).read_bytes()).hexdigest(),ledger_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),expected_pair_count=len(expected),pairs=pairs)
(D/'index.json').write_text(json.dumps(index,indent=2)+'\n')
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',MPLCONFIGDIR='/tmp/pipeline-matplotlib')
commands=[[sys.executable,str(R/'python-scripts/compare_runs.py'),str(D/'index.json'),'--output',str(D)],*[ [sys.executable,str(R/'python-scripts'/name),str(D)] for name in ['draw_comparison.py','draw_comparison_timeseries.py'] ],[sys.executable,str(A/'write_comparison_report.py'),str(D)]]
if a.family=='isolated':commands.insert(-1,[sys.executable,str(R/'python-scripts/draw_partition_comparison.py'),str(D)])
with (D/'analysis.log').open('w') as out:
 for command in commands:subprocess.run(command,env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
print(status,D,flush=True)
