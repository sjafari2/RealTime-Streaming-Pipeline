"""Plot recorded pilot outcomes alongside predictions saved before release."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def plot(source,destination):
    data=json.loads(source.read_text());rows=sorted(data['rows'],key=lambda r:(r['added_hold_seconds'],r['response'],r['run']))
    x=np.arange(len(rows));colors=['#326eaa' if r['response']=='baseline' else '#d7802b' for r in rows]
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    fig,(ax,bx)=plt.subplots(2,1,figsize=(12,7),sharex=True,gridspec_kw={'height_ratios':[3,1]})
    ax.bar(x,[r['completion_p99_seconds'] for r in rows],color=colors,alpha=.8,label='Observed completion p99')
    ax.scatter(x,[r['predicted_age_blind_p99'] for r in rows],marker='x',s=65,color='#222222',label='Diagnostic without age',zorder=5)
    ax.scatter(x,[r['predicted_age_aware_p99'] for r in rows],marker='D',s=38,facecolor='white',edgecolor='#222222',label='Diagnostic including age',zorder=6)
    ax.axhline(20,color='#a52834',ls='--',lw=1.4,label='20-second pilot objective')
    ax.set_ylabel('Waiting-cohort completion p99 (s)')
    ax.set_title('Kafka queue-age pilot: observed latency and pre-release predictions',loc='left',fontweight='bold')
    ax.legend(ncol=2,fontsize=9);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    bx.bar(x,[r['unfinished'] for r in rows],color=colors,alpha=.8)
    bx.set_ylabel('Unfinished\nat cutoff');bx.grid(axis='y',alpha=.2);bx.set_axisbelow(True)
    for i,r in enumerate(rows):
        bx.text(i,max(r['unfinished'],1),str(r['unfinished']),ha='center',va='bottom',fontsize=9)
    bx.set_ylim(0,max(10,max(r['unfinished'] for r in rows)*1.4))
    bx.set_xticks(x)
    bx.set_xticklabels([('Baseline' if r['response']=='baseline' else 'Targeted 6')+'\nRun '+str(r['run'])+'\nHold +'+str(r['added_hold_seconds'])+' s' for r in rows])
    fig.text(.06,.015,'Explicit candidate assignments; six pods prestarted. Bars exclude unfinished messages; counts are shown separately.\nBoth latency predictors are added diagnostics, not forecasts emitted by the published rule. Actual ages are retained in the report.',fontsize=9,color='#444444')
    fig.tight_layout(rect=[0,.075,1,1]);fig.savefig(destination,dpi=180);plt.close(fig)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('source',type=Path);p.add_argument('destination',type=Path)
    a=p.parse_args();plot(a.source,a.destination)
