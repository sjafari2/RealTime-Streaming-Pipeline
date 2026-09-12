"""Draw paired preliminary outcomes from compare_runs.py's saved summary."""
import argparse
import json
from pathlib import Path
import platform

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('comparison_directory',type=Path)
a=p.parse_args();D=a.comparison_directory
s=json.loads((D/'comparison-summary.json').read_text())
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
                     'svg.fonttype':'none','savefig.facecolor':'white'})
colors=['#24648a','#b95728','#507344','#815996']
metrics=[('unfinished_fraction',100,'Unfinished at drain','Cohort %'),
         ('admitted_cohort_completion_p99_seconds',1,'Recorded completion p99','Seconds, completed cohort only'),
         ('mean_processing_backlog_offsets',1,'Covered-interval mean backlog','Completion-frontier offsets'),
         ('evaluation_and_drain_requested_cpu_seconds',1/60,'Consumer CPU requests','Core-minutes, evaluation + drain')]
fig,axes=plt.subplots(2,2,figsize=(9,7),layout='constrained')
for ax,(metric,factor,title,ylabel) in zip(axes.flat,metrics):
    displayed=[]
    for i,pair in enumerate(s['pairs']):
        runs=[pair['runs'][k] for k in ('none','scale')]
        eligible=not pair['compatibility_failures'] and all(r['evidence_valid'] for r in runs)
        if metric=='mean_processing_backlog_offsets':
            eligible=eligible and all(r['metrics']['lag_covered_fraction']>=.9 for r in runs)
        if metric=='evaluation_and_drain_requested_cpu_seconds':
            eligible=eligible and all(r['metrics']['evaluation_and_drain_resource_coverage']>=.95 for r in runs)
        values=[r['metrics'].get(metric) for r in runs]
        y=[v*factor if v is not None else np.nan for v in values]
        displayed.extend(v for v in y if np.isfinite(v))
        color=colors[i%len(colors)]
        ax.plot([0,1],y,marker='o' if eligible else 'x',color=color,
                ls='-' if eligible else ':',label=pair['pair']+('' if eligible else ' (coverage/validity flag)'),lw=1.5)
    ax.set(title=title,ylabel=ylabel,xticks=[0,1],xticklabels=['Keep 3 consumers','Schedule 3 → 6'])
    ax.set_xlim(-.2,1.2)
    # Leave room above nearly equal values, and make an all-zero outcome clear.
    upper=max(displayed,default=0)
    ax.set_ylim(0,upper*1.25 if upper>0 else 1)
    if metric=='unfinished_fraction' and displayed and upper==0:
        ax.text(.5,.5,'0% unfinished in all shown runs',ha='center',transform=ax.transAxes,fontsize=9)
    ax.grid(axis='y',alpha=.2)
    ax.legend(fontsize=8)
fig.suptitle(f"Preliminary action comparison · {len(s['pairs'])} matched seed pair(s)\neach line connects the two runs for one seed",fontsize=12)
fig.supxlabel('Read conditional latency with unfinished outcomes. Dotted lines are retained but excluded from that metric’s aggregate.\nRequested CPU is not measured whole-cluster cost. Short pilots on shared nodes do not establish general superiority.',fontsize=8)
for suffix in ('png','svg','pdf'):
    fig.savefig(D/('paired-outcomes.'+suffix),dpi=190,bbox_inches='tight')
plt.close(fig)
svg=D/'paired-outcomes.svg';svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
(D/'plot-environment.json').write_text(json.dumps(dict(python=platform.python_version(),matplotlib=matplotlib.__version__,numpy=np.__version__),indent=2)+'\n')
print('Saved paired outcome plots:',D)
