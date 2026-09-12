"""Plot saved partition backlog and exact evaluation rates for a matched pair."""
import argparse
import hashlib
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

p=argparse.ArgumentParser(description=__doc__);p.add_argument('comparison_directory',type=Path);p.add_argument('--partition',type=int,default=0);a=p.parse_args();D=a.comparison_directory
summary=json.loads((D/'comparison-summary.json').read_text())
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none','savefig.facecolor':'white'})
colors={'none':'#24648a','scale':'#b95728'};labels={'none':'Keep 3','scale':'Schedule 3 → 6'}

def load(path,sources):
    sources[str(path.resolve())]=hashlib.sha256(path.read_bytes()).hexdigest()
    return json.loads(path.read_text())

for pair in summary['pairs']:
    if pair['compatibility_failures']:raise ValueError('Inspect incompatible pairs before plotting partition details')
    sources={};details={};fig,axes=plt.subplots(2,1,figsize=(9,7),layout='constrained')
    for action,row in pair['runs'].items():
        run=Path(row['directory']);m=load(run/'manifest.json',sources);lag=load(run/'lag-summary.json',sources)
        raw=load(run/'prometheus.json',sources);o=load(run/'outcome-summary.json',sources)
        assert int(m['config']['TOPIC_COUNT'])==1
        count=int(m['config']['NUM_PARTITIONS']);assert 0<=a.partition<count
        part=m['config']['TOPIC_TITLE']+'_0/'+str(a.partition)
        values={}
        for series in raw['data']['result']:
            k=series['metric']
            if k.get('__name__')!='consumer_processing_backlog' or k.get('run_id')!=m['run_id']:continue
            key=(k['topic']+'/'+k['partition'],k['pod']+'/'+k['incarnation'])
            for t,v in series.get('values',[]):
                index=(t,*key)
                if index in values:raise ValueError('Duplicate process/partition backlog series')
                values[index]=float(v)
        x=[];y=[];previous=None;owner_periods=[]
        start=m['evaluation_start_epoch'];duration=m['producer_end_epoch']-start
        for snap in lag['snapshots']:
            t=snap['timestamp'];relative=t-start
            if not snap['valid']:
                x.append(relative);y.append(math.nan);previous=None;continue
            # Join only the owner selected by the validated lag snapshot. Check
            # the entire partition sum against the saved aggregate before use.
            selected={k:values[(t,k,owner)] for k,owner in snap['owners'].items()}
            if not all(math.isfinite(v) and v>=0 for v in selected.values()):raise ValueError('Invalid backlog in a valid snapshot')
            if not math.isclose(sum(selected.values()),snap['processing_backlog'],abs_tol=1e-7):raise ValueError('Partition sum differs from saved group backlog')
            owner=snap['owners'][part]
            continuous=previous is not None and previous['owners'][part]==owner and snap.get('growth_offsets_per_second') is not None
            if previous is not None and not continuous:x.append(math.nan);y.append(math.nan)
            x.append(relative);y.append(selected[part])
            if not owner_periods or owner_periods[-1]['owner']!=owner or previous is None:
                owner_periods.append(dict(owner=owner,first_valid_evaluation_second=relative,last_valid_evaluation_second=relative))
            else:owner_periods[-1]['last_valid_evaluation_second']=relative
            previous=snap
        axes[0].plot(x,y,color=colors[action],lw=1.5,label=labels[action])
        parts=o['partition_metrics']['partitions'];selected=next(z for z in parts if z['topic_index']==0 and z['partition']==a.partition)
        rest=[z for z in parts if z['partition']!=a.partition]
        admissions=[selected['admitted_per_second'],sum(z['admitted_per_second'] for z in rest)]
        completions=[selected['unique_completions_per_second'],sum(z['unique_completions_per_second'] for z in rest)]
        assert math.isclose(sum(completions),o['useful_throughput_per_second'],abs_tol=1e-7)
        offset=-.27 if action=='none' else .09;positions=np.arange(2)
        axes[1].bar(positions+offset,admissions,width=.18,color=colors[action],alpha=.4,hatch='//',label=labels[action]+' admissions')
        axes[1].bar(positions+offset+.18,completions,width=.18,color=colors[action],label=labels[action]+' distinct completions')
        details[action]=dict(run_id=m['run_id'],selected_partition_metrics=selected,other_partitions_admitted=sum(z['admitted_messages'] for z in rest),other_partitions_unfinished=sum(z['incomplete_messages'] for z in rest),hot_owner_observation_periods=owner_periods,lag_coverage=lag['covered_fraction'])
    anchor=pair['runs']['scale']['intervention']['after_evaluation_start_seconds']
    axes[0].axvline(anchor,color='#444444',ls='--',lw=1,label='Scheduled decision time')
    axes[0].set(xlim=(0,duration),ylim=(0,None),xlabel='Seconds from evaluation start',ylabel='Completion-frontier offsets',title=f'Selected partition {a.partition} processing backlog · invalid/transition observations remain gaps')
    axes[0].legend(fontsize=8)
    axes[1].set(xticks=[0,1],xticklabels=[f'Partition {a.partition}',f'Other {count-1} partitions (sum)'],ylabel='Messages / evaluation second',title='Acknowledged admissions and distinct completions during evaluation')
    axes[1].legend(fontsize=8,ncol=2)
    for ax in axes:ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.suptitle(f"Partition-level evidence · pair {pair['pair']} · seed {pair['runs']['none']['workload_seed']}",fontsize=12)
    coverage='; '.join(labels[k]+f": lag coverage {100*v['lag_coverage']:.1f}%" for k,v in details.items())
    fig.supxlabel(coverage+'\nCompletion rates can include messages admitted before evaluation; unfinished outcomes use the evaluation-admission cohort.\nChanging owners or shared-node performance can affect progress. Scaling does not divide one partition between concurrent owners.',fontsize=8)
    stem=f"pair-{pair['pair']}-partition-{a.partition}"
    for suffix in ('png','svg','pdf'):fig.savefig(D/(stem+'.'+suffix),dpi=190,bbox_inches='tight')
    plt.close(fig)
    svg=D/(stem+'.svg');svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    (D/(stem+'-data.json')).write_text(json.dumps(dict(pair=pair['pair'],selected_partition=a.partition,runs=details,sources=sources),indent=2,allow_nan=False)+'\n')
print('Saved validated partition-level figures:',D)
