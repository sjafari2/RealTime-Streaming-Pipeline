"""Preserve a reviewed comparison snapshot with hashes; raw events stay in results."""
import argparse,hashlib,json,shutil,subprocess,time
from pathlib import Path
R=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('family',choices=['sustained','short','isolated','low-input']);a=p.parse_args()
src=A/'comparisons'/a.family;dst=R/'experiment-records/campaign-20260912/comparisons'/a.family
dst.mkdir(parents=True,exist_ok=True)
for f in sorted(src.iterdir()):
 if f.is_file() and f.suffix in ('.json','.csv','.md','.pdf','.png','.svg'):shutil.copy2(f,dst/f.name)
scripts=[R/'python-scripts'/n for n in ('compare_runs.py','draw_comparison.py','draw_comparison_timeseries.py','draw_partition_comparison.py')]+[A/'write_comparison_report.py']
metadata=dict(status=json.loads((src/'index.json').read_text())['status'],packaged_epoch=time.time(),git_revision=subprocess.check_output(['git','-C',str(R),'rev-parse','HEAD'],text=True).strip(),scripts={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in scripts},files=[dict(path=str(f.relative_to(dst)),bytes=f.stat().st_size,sha256=hashlib.sha256(f.read_bytes()).hexdigest()) for f in sorted(dst.rglob('*')) if f.is_file() and f.name!='comparison-files.json'])
(dst/'comparison-files.json').write_text(json.dumps(metadata,indent=2)+'\n')
print(dst)
