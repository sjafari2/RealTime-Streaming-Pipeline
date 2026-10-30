"""Plot observed deadline outcomes separately from diagnostic predictions."""
import argparse
import json
from pathlib import Path


def plots(ablation_path, controlled_path, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    output.mkdir(exist_ok=True,parents=True)
    ablation=json.loads(ablation_path.read_text())
    fig,ax=plt.subplots(figsize=(8.4,4.5),layout='constrained')
    labels=['A: pooled\nwithout age','B: individual\nwithout age',
            'C: pooled\nwith age','D: individual\nwith age']
    x=np.arange(4)
    for i,pilot in enumerate(ablation['pilots']):
        y=[pilot['error_counts'][n]['false_successes'] for n in 'ABCD']
        bars=ax.bar(x+(i-.5)*.34,y,.34,label=f'Prior pilot {i+1} (8 episodes)',color=['#246b9e','#dc9132'][i])
        ax.bar_label(bars,padding=3)
    ax.set_xticks(x,labels);ax.set_ylim(0,5);ax.set_yticks(range(6))
    ax.set_ylabel('False predictions of meeting the deadline objective')
    ax.set_title('Saved-data ablation: capacity and message age')
    ax.legend(frameon=False,loc='upper right')
    fig.savefig(output/'capacity-age-ablation.png',dpi=180);plt.close(fig)
    if not controlled_path:return
    result=json.loads(controlled_path.read_text());rows=result['rows']
    conditions=[(h,r) for h in [0,8] for r in [1,2]]
    arms=['redistribute3','targeted6']; names=['3-consumer assignment','6-consumer assignment']
    colors=['#246b9e','#d47719']
    fig,axes=plt.subplots(2,2,figsize=(12.8,8.5),layout='constrained')
    x=np.arange(4)
    for j,(arm,name,color) in enumerate(zip(arms,names,colors)):
        selected=[next((r for r in rows if (r['hold_seconds'],r['run'])==c and r['response']==arm),None) for c in conditions]
        for ax,key in [(axes[0,0],'completion_p99_seconds'),(axes[0,1],'deadline_miss_percent')]:
            values=[r[key] if r else float('nan') for r in selected]
            bars=ax.bar(x+(j-.5)*.36,values,.36,color=color,label=name)
            ax.bar_label(bars,labels=[f'{v:.2f}' if v==v else '' for v in values],padding=3,fontsize=8)
    for ax in axes[0]:
        ax.set_xticks(x,['Younger\nRun 1','Younger\nRun 2','Older\nRun 1','Older\nRun 2'])
        ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
    axes[0,0].axhline(20,color='#555555',linestyle=':',label='20-second objective')
    axes[0,0].set_ylabel('Completion p99 (seconds)')
    axes[0,0].set_ylim(0,max(30,max(r['completion_p99_seconds'] for r in rows)+7))
    axes[0,0].set_title('Performance: initial waiting cohort')
    axes[0,0].legend(frameon=False,fontsize=8,loc='upper left')
    axes[0,1].axhline(1,color='#555555',linestyle=':',label='Maximum allowed: 1%')
    axes[0,1].set_ylabel('Messages exceeding 20 seconds (%)')
    axes[0,1].set_title('Performance: deadline violations');axes[0,1].legend(frameon=False,fontsize=8)
    for ax,hold in zip(axes[1],[0,8]):
        for row in rows:
            if row['hold_seconds']!=hold:continue
            j=arms.index(row['response']);points=row['timeline']
            ax.plot([r['seconds'] for r in points],[r['pending_messages'] for r in points],
                color=colors[j],linestyle='-' if row['run']==1 else '--',linewidth=1.6,
                label=f'{3 if j==0 else 6} consumers, Run {row["run"]}')
        ax.axvline(90,color='#777777',linestyle=':',label='Input stops')
        ax.set_title('Pending work: '+('younger' if hold==0 else 'older')+' initial messages')
        ax.set_xlabel('Seconds since processing release');ax.set_ylabel('Messages pending completion')
        ax.set_ylim(bottom=0);ax.grid(alpha=.15);ax.legend(frameon=False,fontsize=8)
    title='Controlled message ages under preinstalled assignments'
    if len(rows)!=8:
        title+='\nInterrupted block: six completed episodes; younger Run 2 not completed'
        for ax in axes[0]:
            ax.text(1,.04,'Not completed',ha='center',va='bottom',fontsize=8,
                    transform=ax.get_xaxis_transform())
    fig.suptitle(title,fontsize=14)
    fig.savefig(output/'controlled-age-outcomes.png',dpi=180);plt.close(fig)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ablation',type=Path,required=True)
    parser.add_argument('--controlled',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();plots(args.ablation,args.controlled,args.output)
