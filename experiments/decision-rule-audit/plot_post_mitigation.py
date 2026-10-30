"""Plot decision-cohort clearance and observed service from the saved trials."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

ARMS = ['keep3','redistribute3','scale_redistribute6','kafka_scale6']
LABELS = {'keep3':'Keep 3','redistribute3':'Redistribute 3',
          'scale_redistribute6':'Targeted scale 6','kafka_scale6':'Kafka scale 6'}
COLORS = ['#285f8f','#8ab6d6','#e0a23c','#b8b8b8']


def select(rows, condition, run=None):
    return sorted([r for r in rows if r['backlog_condition']==condition and
                   (run is None or r['run_number']==run)],
                  key=lambda r:(ARMS.index(r['arm']),r['run_number']))


def timelines(rows, output):
    fig,axes = plt.subplots(1,2,figsize=(14,6.5),sharex=True)
    for ax,condition in zip(axes,['smaller','larger']):
        selected = select(rows,condition)
        for y,r in enumerate(selected):
            a = r['activation_after_decision']
            clear = r['cohorts']['pending_at_decision']['all_completed_seconds']
            recovery = r['original_recovery_after_decision']['confirmation_after_anchor_seconds']
            if y//2 % 2 == 0:
                ax.axhspan(y-.45,y+.45,color='#f1f5f9',zorder=0)
            ax.plot([0,a],[y,y],color='#a7b2bc',lw=3,zorder=1)
            if r['release_requested_after_decision'] is not None:
                ax.plot(r['release_requested_after_decision'],y,marker='|',color='#b46724',ms=17,mew=2)
            ax.plot(a,y,marker='o',color='#285f8f',ms=7)
            if clear is not None:
                ax.plot(clear,y,marker='D',color='#99559b',ms=6)
            if recovery is not None:
                ax.plot(recovery,y,marker='s',color='#287b51',ms=7)
            else:
                ax.plot(540,y,marker='>',mfc='none',mec='#287b51',ms=8)
        ax.set_yticks(range(len(selected)),[LABELS[r['arm']]+' · Run '+str(r['run_number']) for r in selected])
        ax.invert_yaxis()
        ax.set_title(condition.capitalize()+' starting backlog',fontsize=12)
        ax.set_xlabel('Seconds after scheduled decision')
        ax.xaxis.set_major_locator(MultipleLocator(100))
        ax.set_xlim(-8,557)
        ax.grid(axis='x',alpha=.2)
    handles = [Line2D([],[],marker='|',color='#b46724',ls='',ms=14,label='Transfer release requested'),
               Line2D([],[],marker='o',color='#285f8f',ls='',label='Assignment confirmed'),
               Line2D([],[],marker='D',color='#99559b',ls='',label='All decision-time waiting messages finished'),
               Line2D([],[],marker='s',color='#287b51',ls='',label='Whole-backlog recovery confirmed'),
               Line2D([],[],marker='>',mfc='none',mec='#287b51',ls='',label='Recovery unconfirmed at evaluation end')]
    fig.suptitle('Waiting-cohort clearance and pipeline recovery are different events',fontsize=15,y=.98)
    fig.legend(handles=handles,loc='lower center',ncol=2,frameon=False,fontsize=9,bbox_to_anchor=(.5,.045))
    fig.text(.5,.015,'Keep 3 uses the decision as its reference. Recovery requires low processing backlog for 30 valid seconds during continuing input.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.20,1,.94),w_pad=2)
    fig.savefig(output/'cohort-and-recovery-timeline.png',dpi=180)
    plt.close(fig)


def active_throughput(rows, output):
    fig,axes = plt.subplots(2,2,figsize=(12,8),sharey=True)
    for ax,(condition,run) in zip(axes.flat,[('smaller',1),('smaller',2),('larger',1),('larger',2)]):
        selected = select(rows,condition,run)
        for x,r in enumerate(selected):
            interval = next(w for w in r['intervals'] if w['name']=='First 30 s active')
            initial = interval['decision_groups'].get('pending_at_decision',0)
            at_active = interval['activation_cohort_completions']
            newer = interval['produced_after_activation_completions']
            other = interval['distinct_completions']-at_active-newer
            if min(initial,at_active-initial,newer,other)<0:
                raise ValueError('Overlapping service categories')
            bottom=0
            for n,color in zip([initial,at_active-initial,newer,other],COLORS):
                height=n/interval['seconds']
                ax.bar(x,height,bottom=bottom,color=color,width=.65)
                bottom+=height
        ax.axhline(600,color='#444',ls='--',lw=1)
        ax.set_xticks(range(4),[LABELS[r['arm']].replace(' ','\n',1)+'\nA = '+str(round(r['activation_after_decision']))+' s'
                              for r in selected],fontsize=9)
        ax.set_title(condition.capitalize()+' backlog · Run '+str(run),fontsize=12)
        ax.set_ylabel('Throughput (msg/s)')
        ax.yaxis.set_major_locator(MultipleLocator(500))
        ax.grid(axis='y',alpha=.18)
        ax.set_axisbelow(True)
    labels = ['Waiting since scheduled decision','Later messages waiting at activation',
              'Created after activation','Other (including later acknowledgments)']
    handles=[plt.Rectangle((0,0),1,1,color=c) for c in COLORS]
    fig.legend(handles,labels,loc='lower center',ncol=2,frameon=False,fontsize=9,bbox_to_anchor=(.5,.045))
    fig.suptitle('Performance metrics: throughput during the first 30 seconds after activation',fontsize=14,y=.98)
    fig.text(.5,.015,'A is seconds from the scheduled decision to assignment confirmation. Dashed line: continuing input, 600 msg/s.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.15,1,.95))
    fig.savefig(output/'post-activation-throughput.png',dpi=180)
    plt.close(fig)


def consumer_service(rows, output):
    fig,axes = plt.subplots(2,2,figsize=(12,8),sharey=True)
    maximum = max(c['observed_start_rate'] for r in rows
                  if r['arm'] in ('redistribute3','scale_redistribute6')
                  for c in r['first_30_seconds']['consumers'])
    for ax,(condition,run) in zip(axes.flat,[('smaller',1),('smaller',2),('larger',1),('larger',2)]):
        selected=[r for r in select(rows,condition,run) if r['arm'] in ('redistribute3','scale_redistribute6')]
        positions=[]; labels=[]
        for r,base in zip(selected,[0,4]):
            parts=r['first_30_seconds']['partitions']
            for i,c in enumerate(sorted(r['first_30_seconds']['consumers'],key=lambda c:c['pod'])):
                hot={p['partition'] for p in parts if p['high_input']}
                counts=c['partitions']
                h=sum(p['processing_starts'] for p in counts if p['partition'] in hot)/30
                cold=sum(p['processing_starts'] for p in counts if p['partition'] not in hot)/30
                ax.bar(base+i,h,color=COLORS[0],width=.7)
                ax.bar(base+i,cold,bottom=h,color=COLORS[2],width=.7)
                positions.append(base+i);labels.append(c['pod'].rsplit('-',1)[-1])
        ax.set_xticks(positions,labels)
        ax.axvline(3,ls=':',color='#888',lw=1)
        ax.set_title(condition.capitalize()+' backlog · Run '+str(run),fontsize=12)
        ax.set_xlabel('Redistribute 3             Targeted scale 6\nConsumer ordinal (0-based)')
        ax.set_ylabel('Recorded processing starts/s')
        ax.set_ylim(0,maximum*1.10)
        ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
    fig.suptitle('Observed consumer service: first 30 seconds after activation',fontsize=14,y=.98)
    fig.legend([plt.Rectangle((0,0),1,1,color=COLORS[0]),plt.Rectangle((0,0),1,1,color=COLORS[2])],
               ['Twelve high-input partitions','Other partitions'],loc='lower center',ncol=2,frameon=False,bbox_to_anchor=(.5,.045))
    fig.text(.5,.015,'These are observed processing rates, not calibrated capacities. Repeated processing, if present, remains physical work.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.14,1,.95))
    fig.savefig(output/'post-activation-consumer-service.png',dpi=180)
    plt.close(fig)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results',type=Path,required=True)
    args=parser.parse_args()
    data=json.loads(args.results.read_text())
    if data['status']!='complete' or data['analyzed_trials']!=16:
        raise ValueError('The figures require the complete 16-trial analysis')
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    for plot in [timelines,active_throughput,consumer_service]:
        plot(data['runs'],args.results.parent)


if __name__=='__main__':
    main()
