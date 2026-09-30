#!/usr/bin/env python3
"""Plot outstanding work and observed ownership from retained comparison evidence."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/handoff-diagnostics-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from evidence_io import event_paths, open_events

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
LABELS={'keep3':'Keep 3','scale6':'Scale + targeted redistribution',
        'redistribute3':'Redistribute within 3'}
COLORS=['#2563a6','#dc862d','#21875e','#9a4eaa','#bc4148','#5d7390']


def outstanding_counts(acknowledgments,completions,times):
    """Count records acknowledged by t and not completed by t, including warm-up."""
    if not set(completions)<=set(acknowledgments):
        raise ValueError('Completion without an acknowledged message identity')
    ack=np.array(list(acknowledgments.values()))
    # If completion precedes its ACK callback, both enter the count together.
    finished=np.array([max(t,completions.get(k,float('inf'))) for k,t in acknowledgments.items()])
    return (np.searchsorted(np.sort(ack),times,side='right')-
            np.searchsorted(np.sort(finished),times,side='right'))


def load(run):
    directory=ROOT/run['raw_evidence']
    manifest=json.loads((directory/'manifest.json').read_text())
    lag=json.loads((directory/'lag-summary.json').read_text())
    ack={};done={};hashes={}
    for path in event_paths(directory):
        hashes[str(path.relative_to(directory))]=hashlib.sha256(path.read_bytes()).hexdigest()
        with open_events(path) as stream:
            for line in stream:
                event=json.loads(line)
                if event['event']=='acknowledged':target,timestamp=ack,event['timestamp']
                elif event['event']=='completed':target,timestamp=done,event['completion_timestamp']
                else:continue
                if event['message_id'] in target:raise ValueError('Duplicate message event identity')
                target[event['message_id']]=timestamp
    start,end=manifest['evaluation_start_epoch'],manifest['producer_end_epoch']
    times=np.arange(start,end+.1,2)
    values=outstanding_counts(ack,done,times)
    valid=[x for x in lag['snapshots'] if x['valid'] and start<=x['timestamp']<=end]
    if not valid:raise ValueError('No complete valid ownership observation')
    last=valid[-1];hot=set(map(int,manifest['config']['SKEW_PARTITION'].split(',')))
    owners={key:owner.split('/')[0] for key,owner in last['owners'].items()}
    hot_counts={pod:sum(owner==pod and int(key.rsplit('/',1)[1]) in hot for key,owner in owners.items())
                for pod in sorted(set(owners.values()))}
    if sum(hot_counts.values())!=len(hot):raise ValueError('Incomplete observed hot-partition map')
    marks=run.get('transition_times_evaluation_minutes',{})
    result=dict(run_id=run['run_id'],arm=run['arm'],run_number=run['run_number'],
                evaluation_seconds=end-start,hot_partitions=sorted(hot),
                last_ownership_epoch=last['timestamp'],final_hot_partitions_per_owner=hot_counts,
                outstanding_evaluation_seconds=(times-start).tolist(),outstanding_messages=values.tolist(),
                event_file_sha256=hashes,summary_file_sha256={name:hashlib.sha256((directory/name).read_bytes()).hexdigest()
                    for name in ['manifest.json','lag-summary.json','outcome-summary.json']})
    return result,marks


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('record',type=Path);parser.add_argument('output',type=Path)
    parser.add_argument('--prefix',required=True)
    args=parser.parse_args();comparison=json.loads((args.record/'comparison.json').read_text())
    runs=sorted(comparison['runs'],key=lambda x:(x['run_number'],x['arm']))
    if len(runs)!=4 or any(x['arm'] not in LABELS for x in runs):
        raise ValueError('Expected two runs for each condition in a five-minute comparison')
    rows=[load(run) for run in runs]
    if any(abs(row['evaluation_seconds']-300)>1e-3 for row,_ in rows):
        raise ValueError('Expected five-minute evaluation boundaries')
    args.output.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    for metric in ['outstanding','ownership']:
        fig,axes=plt.subplots(2,2,figsize=(12,8.3),sharex=True,sharey=True)
        for ax,(row,marks) in zip(axes.flat,rows):
            if metric=='outstanding':
                ax.plot(np.array(row['outstanding_evaluation_seconds'])/60,row['outstanding_messages'],color=COLORS[0],lw=1.3)
                if marks:
                    ax.axvline(1,color='#777',ls=':',lw=.8)
                    ax.axvspan(marks['release_requested'],marks['active_verified'],color='#64748b',alpha=.13)
                ax.set(xlabel='Evaluation time (minutes)',ylabel='Acknowledged but unfinished messages',xlim=(0,5),ylim=(0,None))
            else:
                counts=row['final_hot_partitions_per_owner'];pods=['consumer-sts-'+str(i) for i in range(6)]
                bars=ax.bar(range(6),[counts.get(p,0) for p in pods],color=COLORS,width=.65)
                for bar,pod in zip(bars,pods):
                    ax.annotate(str(counts[pod]) if pod in counts else 'n/a',
                                (bar.get_x()+bar.get_width()/2,bar.get_height()),xytext=(0,3),textcoords='offset points',ha='center',fontsize=9)
                ax.set(ylabel='Hot partitions',ylim=(0,14),xticks=range(6),xticklabels=['C'+str(i) for i in range(6)])
            ax.set_title(LABELS[row['arm']]+' - Run '+str(row['run_number']))
            ax.tick_params(labelbottom=True,labelleft=True);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
        title='Acknowledged but unfinished work' if metric=='outstanding' else 'Observed hot-partition ownership at the end of evaluation'
        fig.suptitle(title+' - 700 messages/s',fontweight='bold',fontsize=15)
        note='Each trial: 1 min warm-up + 5 min evaluation + 2 min drain. Common scales.'
        if metric=='outstanding':note+='\nDistinct ACK/completion events, including warm-up; this is not broker offset lag.\nDotted line: scheduled intervention. Shading: release requested to active verified.'
        else:note+='\nLast complete valid ownership snapshot; n/a means no partition ownership in that snapshot.\nC0-C5 identify consumers. Equal partition counts do not imply equal processing load.'
        fig.text(.07,.02,note,fontsize=9,color='#555');fig.tight_layout(rect=(0,.12,1,.94))
        for ext in ['png','pdf']:fig.savefig(args.output/(args.prefix+'-'+metric+'.'+ext),dpi=180,facecolor='white')
        plt.close(fig)
    payload=dict(definitions=dict(outstanding='Distinct identities with acknowledgment callback timestamp <= t and completion timestamp > t; includes warm-up. Reconstructed from events, not interpolated offset lag.',ownership='Hot-partition counts from the final complete valid ownership snapshot during evaluation.'),runs=[row for row,_ in rows])
    (args.output/'additional-diagnostics.json').write_text(json.dumps(payload,indent=2,allow_nan=False)+'\n')
    print('Saved outstanding-work and ownership figures for',args.prefix)


if __name__=='__main__':main()
