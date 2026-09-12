"""Compare saved monitoring curves at the same relative evaluation times."""
import argparse
import hashlib
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

p=argparse.ArgumentParser(description=__doc__);p.add_argument('comparison_directory',type=Path)
a=p.parse_args();D=a.comparison_directory
s=json.loads((D/'comparison-summary.json').read_text())
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none','savefig.facecolor':'white'})
colors={'none':'#24648a','scale':'#b95728'}
labels={'none':'Keep 3 consumers','scale':'Schedule 3 → 6'}
sources={}

def load(path):
    sources[str(path.resolve())]=hashlib.sha256(path.read_bytes()).hexdigest()
    return json.loads(path.read_text())

for pair in s['pairs']:
    fig,axes=plt.subplots(2,1,figsize=(9,7),layout='constrained')
    durations=[];coverage=[]
    for action,row in pair['runs'].items():
        run=Path(row['directory']);m=load(run/'manifest.json');lag=load(run/'lag-summary.json')
        start=m['evaluation_start_epoch'];duration=m['producer_end_epoch']-start;durations.append(duration)
        coverage.append(f"{labels[action]}: lag coverage {100*lag['covered_fraction']:.1f}%")
        x=[];y=[];previous=None
        for current in lag['snapshots']:
            if previous and previous['valid'] and current['valid'] and current.get('growth_offsets_per_second') is None:
                x.append(math.nan);y.append(math.nan)
            x.append(current['timestamp']-start);y.append(current.get('processing_backlog',math.nan) if current['valid'] else math.nan)
            previous=current
        axes[0].plot(x,y,color=colors[action],lw=1.5,label=labels[action])
        spec=load(run/'plots/queries.json')
        data=load(run/'plots/completion_rate.json')
        for series in data['data']['result']:
            x=[];y=[];previous=None
            for t,v in series['values']:
                if previous is not None and t-previous>1.5*spec['step']:x.append(math.nan);y.append(math.nan)
                x.append(t-start);y.append(float(v));previous=t
            axes[1].plot(x,y,color=colors[action],lw=1.5,label=labels[action])
    if max(durations)-min(durations)>.01:raise ValueError('Incompatible evaluation durations')
    anchor=pair['runs']['scale']['intervention']['after_evaluation_start_seconds']
    for ax in axes:
        ax.set_xlim(0,durations[0]);ax.set_ylim(bottom=0);ax.axvline(anchor,color='#444444',ls='--',lw=1,label='Scheduled decision time')
        ax.set_xlabel('Seconds from evaluation start');ax.grid(axis='y',alpha=.2);ax.legend(fontsize=8)
    axes[0].set(title='Processing backlog · gaps are invalid or discontinuous observations',ylabel='Completion-frontier offsets')
    axes[1].set(title='Completion attempt rate · rolling 30-second estimate',ylabel='Attempts / second')
    target=float(m['config']['TARGET_RATE'])*int(m['config']['PRODUCER_POD_COUNT'])
    axes[1].axhline(target,color='#777777',ls=':',lw=1,label='Target admission rate')
    axes[1].legend(fontsize=8)
    fig.suptitle(f"Matched pair {pair['pair']} · workload seed {pair['runs']['none']['workload_seed']}\n{int(m['config']['EXP_DURATION_SEC'])} s production · target {target:,.0f} messages/s · {int(m['config']['APP_CPU_ITERATIONS']):,} SHA iterations/message",fontsize=12)
    fig.supxlabel('; '.join(coverage)+'\nReplayed completed attempts can contribute to monitoring rates; exact distinct cohort outcomes are reported separately.',fontsize=8)
    for suffix in ('png','svg','pdf'):fig.savefig(D/(f"pair-{pair['pair']}-timecourse."+suffix),dpi=190,bbox_inches='tight')
    plt.close(fig)
    path=D/f"pair-{pair['pair']}-timecourse.svg";path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines())+'\n')
(D/'timecourse-source-hashes.json').write_text(json.dumps(sources,indent=2)+'\n')
print('Saved paired monitoring time courses:',D)
