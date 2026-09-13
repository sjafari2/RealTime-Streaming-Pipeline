"""Keep the two matched pairs visible rather than pool messages as replications."""
from pathlib import Path
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
R=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent;O=A/'combined-staged';O.mkdir(exist_ok=True)
read=lambda p:json.loads(p.read_text())
families=[('8020','80% across 12 partitions','8020-comparison-20260913','8020-restart'),('single-partition','80% to partition 0','static-startup-executed-20260912','single-partition')]
all_rows=[]
for name,title,old,new in families:
 previous=read(R/'experiment-records'/old/'summary.json')
 current=read(A/new/'records-staged/experiment-records/repetitions-20260913'/name/'summary.json')
 for rep,block in [(1,previous),(2,current)]:
  for row in block['runs']:all_rows.append(dict(workload=name,repetition=rep,**row))
fig,axes=plt.subplots(2,3,figsize=(12,8),layout='constrained')
for i,(name,title,_,_) in enumerate(families):
 for j,(key,label) in enumerate([('unfinished_percent','Unfinished at drain (%)'),('completion_p99_seconds','Completion p99 (seconds)'),('requested_cpu_core_minutes','Observed requested CPU (core-min)')]):
  ax=axes[i,j]
  for action,color,label2,shift in [('none','#355c7d','Keep 3',-.18),('scale','#d87532','Scale to 6',.18)]:
   rows=sorted([r for r in all_rows if r['workload']==name and r['action']==action],key=lambda r:r['repetition'])
   bars=ax.bar([r['repetition']+shift for r in rows],[r[key] for r in rows],width=.35,label=label2,color=color)
   ax.bar_label(bars,labels=[f"{r[key]:.2f}" + ('*' if key=='requested_cpu_core_minutes' and r['resource_coverage']<1 else '') for r in rows],padding=3,fontsize=8)
  ax.set_xticks([1,2],['Pair 1\nscale first','Pair 2\nkeep first']);ax.set_title(title+'\n'+label,fontsize=10);ax.set_ylim(0,max(1,max(r[key] for r in all_rows if r['workload']==name)*1.22));ax.spines[['top','right']].set_visible(False)
axes[0,0].legend();fig.suptitle('Two repetitions per treatment for each workload\nP99 is conditional on completion; * denotes incomplete resource coverage',fontsize=13)
fig.savefig(O/'repetitions.png',dpi=180);fig.savefig(O/'repetitions.svg');plt.close(fig)
f=O/'repetitions.svg';f.write_text('\n'.join(l.rstrip() for l in f.read_text().splitlines())+'\n')
(O/'repetitions-summary.json').write_text(json.dumps(dict(runs=all_rows,notes=['Compare per-run results within each matched pair.','Four runs per workload: two keep-three and two scale-to-six.','Do not count messages as independent experimental repetitions.','No pooled message p99 or statistical significance claim is made.']),indent=2)+'\n')
t='''# Repeated skew comparisons

Both workloads now have two matched-start pairs: two runs keeping three consumers and two scaling to six. The second pair reverses treatment order while keeping the same seed and workload. The first fresh attempt was aborted during empty preparation after a Kubernetes API failure, retained separately, and contributed no performance results.

| Workload | Pair | Keep-three unfinished | Scaling unfinished | Keep-three p99 (s) | Scaling p99 (s) |
|---|---:|---:|---:|---:|---:|
'''
for name,title,_,_ in families:
 for rep in [1,2]:
  rows={r['action']:r for r in all_rows if r['workload']==name and r['repetition']==rep};n,s=rows['none'],rows['scale']
  t+=f"| {title} | {rep} | {n['unfinished_percent']:.2f}% | {s['unfinished_percent']:.2f}% | {n['completion_p99_seconds']:.2f} | {s['completion_p99_seconds']:.2f} |\n"
t+='''
![Repeated comparisons](repetitions.png)

Each trial used 1,500 messages/s total, 2,000 SHA-256 iterations per message, five minutes production including one minute warm-up, and two minutes drain. P99 includes only completed evaluation messages and completion precedes commit acknowledgment. Finishing by drain does not prove recovery under continued input. Resource bars marked * cover observed intervals only; see each report for coverage. Shared-machine contention and new-consumer placement remain uncontrolled; two pairs provide preliminary replication, not a precise population estimate.

[80/20 second pair](8020/README.md) · [Single-partition second pair](single-partition/README.md) · [Aborted empty preparation](aborted-8020-preparation/README.md)
'''
(O/'README.md').write_text(t)
print(t)
