"""Summarize all four conditions and retain gaps and intervention cost definitions."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sys
os.environ.setdefault('MPLCONFIGDIR','/private/tmp/four-condition-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[2]))
sys.path.insert(0,str(ROOT/'python-scripts'))
from evidence_io import event_paths,open_events
from analyze_stability import window
from analyze_execution import resource_integral

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,ROOT/path)
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod
old=module('ownership_figures','experiments/hot-ownership-20260927/summarize.py')
cost=module('intervention_cost','experiments/c2-scaling-20260927/analyze_intervention_cost.py')
ARMS=['keep3','redistribute3','scale_redistribute6','kafka_scale6']
LABELS={'keep3':'Keep 3','redistribute3':'Redistribute within 3','scale_redistribute6':'Scale + targeted redistribution','kafka_scale6':'Scale + Kafka rebalance'}
COLORS=['#2563a6','#dc862d','#21875e','#9a4eaa','#bc4148','#5d7390']

def read(p,n):return json.loads((p/n).read_text())

def stable_six(snapshots,anchor,hold=10):
    first=previous=None
    for row in snapshots:
        if row['timestamp']<anchor:continue
        if not row.get('valid') or len({v.split('/')[0] for v in row['owners'].values()})!=6:
            first=previous=None;continue
        if previous is None or row['owners']!=previous['owners'] or not 0<row['timestamp']-previous['timestamp']<=3:
            first=row['timestamp']
        previous=row
        if row['timestamp']-first>=hold:return dict(first_epoch=first,confirmed_epoch=row['timestamp'],owners=row['owners'])
    return None

def longest_processing_gap(starts,ends,left,right):
    selected=(starts<right)&(ends>left)
    intervals=sorted(zip(starts[selected],ends[selected]))
    cursor=left;best=(left,left)
    for start,end in intervals:
        start=max(left,float(start));end=min(right,float(end))
        if start>cursor and start-cursor>best[1]-best[0]:best=(cursor,start)
        cursor=max(cursor,end)
    if right-cursor>best[1]-best[0]:best=(cursor,right)
    return dict(start_epoch=best[0],end_epoch=best[1],seconds=best[1]-best[0])

def event_cost(path,m,lag):
    arrivals={};done={};by_partition={};callbacks=[];sources={};counts=Counter();duplicate=0
    for p in event_paths(path):
        sources[str(p.relative_to(path))]=hashlib.sha256(p.read_bytes()).hexdigest()
        pod=read(p.parent,'final.json')['pod']
        with open_events(p) as stream:
            for line in stream:
                e=json.loads(line)
                if e['event']=='acknowledged':
                    if e['message_id'] in arrivals:raise ValueError('Duplicate acknowledgment identity')
                    arrivals[e['message_id']]=e['timestamp'];counts[e['partition']]+=1
                elif e['event']=='completed':
                    if e['message_id'] in done:duplicate+=1
                    done[e['message_id']]=(e['processing_start_timestamp'],e['completion_timestamp'])
                    by_partition.setdefault(e['partition'],[]).append((e['processing_start_timestamp'],e['completion_timestamp'],pod,e['offset']))
                elif e['event'] in ('revoke_started','revoke','assign_started','assign','lost','explicit_released','explicit_resumed'):
                    callbacks.append(dict(e,pod=pod))
    if duplicate or not set(done)<=set(arrivals):raise ValueError('Completion identity audit failed')
    ids=list(arrivals);ack=np.array([arrivals[k] for k in ids]);start=np.array([done.get(k,(math.inf,math.inf))[0] for k in ids]);end=np.array([done.get(k,(math.inf,math.inf))[1] for k in ids])
    action=m['intervention']['action'];evaluation=m['evaluation_start_epoch'];finish=m['producer_end_epoch']
    report=dict(event_file_sha256=sources,acknowledged_per_partition=dict(sorted(counts.items())),callbacks=callbacks,action=action)
    marks={}
    if action!='none':
        journal=[json.loads(x) for x in (path/'intervention-events.jsonl').read_text().splitlines()]
        decision=next(e['timestamp'] for e in journal if e['event']=='decision')
        report['decision_epoch']=decision
        report['recovery_from_decision']=[cost.recovery(lag['snapshots'],decision,finish,threshold) for threshold in [50,100,200]]
        if action in ('redistribute','scale_redistribute'):
            marks={e['event']:e['timestamp'] for e in map(json.loads,(path/'explicit-handoff-events.jsonl').read_text().splitlines())}
            left,right=marks['release_requested'],marks['active_verified'];resume=marks['resume_requested']
            last=float(end[end<resume].max());first=float(start[start>=resume].min())
            if np.any((start<first)&(end>last)):raise ValueError('Claimed global pause overlaps application processing')
            report.update(processing_pause_seconds=first-last,processing_pause_start=last,processing_pause_end=first,
                recovery_from_resume=[cost.recovery(lag['snapshots'],resume,finish,t) for t in [50,100,200]],
                additional_unfinished_during_pause=cost.outstanding(ack,end,first)-cost.outstanding(ack,end,last))
            offsets={}
            for row in callbacks:
                if row['event'] in ('explicit_released','explicit_resumed'):offsets.setdefault(row['pod'],{})[row['event']]=row['timestamp']
            report['per_consumer_released_to_resumed_seconds']={p:x['explicit_resumed']-x['explicit_released'] for p,x in offsets.items()}
        else:
            settled=stable_six(lag['snapshots'],decision)
            if settled is None:raise ValueError('Native six-owner assignment never settled')
            left,right=decision,settled['confirmed_epoch'];report['native_stable_ownership']=settled
            marks={'decision':decision,'stable_verified':right}
            initial={x['partition']:x['pod'] for x in m['initial_assignment']}
            final={int(p.rsplit('/',1)[1]):owner.split('/')[0] for p,owner in settled['owners'].items()}
            handovers=[]
            for p in sorted(initial):
                rows=sorted(by_partition.get(p,[]),key=lambda x:x[3])
                observed=[]
                for before,after in zip(rows,rows[1:]):
                    if before[2]==after[2] or after[0]<decision:continue
                    observed.append(dict(partition=p,old_owner=before[2],new_owner=after[2],
                        last_old_completion=before[1],first_new_start=after[0],
                        last_old_offset=before[3],first_new_offset=after[3],
                        between_owner_processing_seconds=after[0]-before[1]))
                handovers.extend(observed)
                if initial[p]!=final[p] and not observed:
                    handovers.append(dict(partition=p,old_owner=initial[p],new_owner=final[p],
                        between_owner_processing_seconds=None,note='No observed completed-record ownership boundary before cutoff'))
            report['moved_partition_intervals']=handovers
        report.update(transition_start_epoch=left,transition_end_epoch=right,transition_seconds=right-left,
            acknowledged_unfinished_at_start=cost.outstanding(ack,end,left),acknowledged_unfinished_at_end=cost.outstanding(ack,end,right),
            net_additional_unfinished=cost.outstanding(ack,end,right)-cost.outstanding(ack,end,left),
            longest_global_no_processing_interval=longest_processing_gap(start,end,left,right))
    x=np.arange(evaluation,finish+.1,2)
    y=[cost.outstanding(ack,end,t) for t in x]
    return report,((x-evaluation)/60,np.array(y)),marks

def load(row,resources,reference,hot):
    p=Path(row['directory']);m=read(p,'manifest.json');o=read(p,'outcome-summary.json');lag=read(p,'lag-summary.json');audit=read(p,'measurement-audit.json')
    assert read(p,'runner-status.json')['status']=='complete' and read(p,'handoff-validation.json')['status']=='passed'
    assert not o['validity_failures'] and not audit['issues']
    cfg=m['config'];assert float(cfg['TARGET_RATE'])*3==700
    assert all(cfg[k]==v for k,v in {'NUM_PARTITIONS':'60','WARMUP_SECONDS':'60','EXP_DURATION_SEC':'660','DRAIN_SECONDS':'120','APP_CPU_ITERATIONS':'2000','APP_DELAY_MS':'0'}.items())
    assert m['placement_reference']==reference
    assert set(map(int,cfg['SKEW_PARTITION'].split(',')))==set(hot)
    initial={x['partition']:x['pod'] for x in m['initial_assignment']};assert all(initial[p]=='consumer-sts-2' for p in hot)
    start,finish=m['evaluation_start_epoch'],m['producer_end_epoch']
    s=dict(arm=row['arm'],run_number=row['run_number'],seed=row['seed'],run_id=m['run_id'],raw_evidence='results/'+m['run_id'],
        admitted=o['admitted_evaluation_cohort'],completed=o['completed_by_drain'],unfinished=o['incomplete_by_drain'],unfinished_percent=100*o['incomplete_fraction'],
        p99_seconds=o['admitted_cohort_completion_p99_seconds'],mean_completion_seconds=o['admitted_cohort_completion_mean_seconds'],
        useful_throughput=o['useful_throughput_per_second'],admitted_rate=o['admitted_messages_per_second'],deadline_miss_fraction=o['deadline_miss_fraction'],
        mean_lag=lag['time_weighted_mean_lag'],peak_lag=lag['peak_sampled_lag'],lag_coverage=lag['covered_fraction'],
        growth_windows={'whole_evaluation':window(lag['snapshots'],start,finish),'last_two_minutes':window(lag['snapshots'],finish-120,finish)},
        process_resources=audit['process_resources'],requested_resources_evaluation_and_drain=resource_integral(resources,start,m['drain_end_epoch'],15),
        initial_ownership=initial,config=cfg,
        evidence_hashes={n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['manifest.json','pipeline-configmap.yaml','outcome-summary.json','lag-summary.json','measurement-audit.json','handoff-validation.json','prometheus.json']})
    event,outstanding,marks=event_cost(p,m,lag);s['intervention_cost']=event
    x,lag_y,_,growth=old.lag_lines(m,lag);skx,sk,skmean=old.skew_lines(m,lag)
    data={'lag':[(x,lag_y)],'growth':[(x,growth)],'skew':[(skx,sk),(skx,skmean)],'outstanding':[outstanding]}
    prom=read(p,'prometheus.json')['data']['result'];consumers=6 if 'scale' in row['arm'] else 3
    for key,name,div in [('cpu','consumer_cpu_percent',100),('memory','consumer_memory_bytes',1024**2)]:
        data[key]=[old.resource_line(prom,name,'consumer-sts-'+str(i),start,finish,div) for i in range(consumers)]
    tx,ys=old.throughput_bins(p,start,finish);data['throughput']=[(tx,y) for y in ys]
    return s,data,{k:(v-start)/60 for k,v in marks.items()}

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('campaign',type=Path);parser.add_argument('output',type=Path);a=parser.parse_args()
    campaign=read(a.campaign,'campaign-status.json')
    if campaign['status']!='complete' or campaign.get('restoration')!='verified' or len(campaign['runs'])!=8:raise ValueError('All eight verified trials and restoration are required')
    resources=[json.loads(x) for x in (a.campaign/'resource-observations.jsonl').read_text().splitlines()]
    rows=sorted(campaign['runs'],key=lambda r:(ARMS.index(r['arm']),r['run_number']))
    runs=[load(r,resources,campaign['reference'],campaign['hot_partitions']) for r in rows]
    a.output.mkdir(parents=True,exist_ok=True)
    payload=dict(campaign_revision=campaign['code_commit'],cost_protocol=campaign['cost_protocol'],hot_partitions=campaign['hot_partitions'],technical=campaign['technical'],preparations=campaign['preparations'],runs=[s for s,_,_ in runs])
    (a.output/'comparison.json').write_text(json.dumps(payload,indent=2,allow_nan=False)+'\n')
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    figures=[]
    for metric,title,ylabel in [('lag','Total lag','Offsets'),('growth','Processing-backlog growth','Offsets/s (30-second change)'),('skew','Partition lag skew','Maximum / mean partition lag'),('outstanding','Acknowledged but unfinished work','Messages'),('cpu','Consumer CPU usage','Process CPU (cores)'),('memory','Consumer memory usage','Process RSS (MiB)'),('throughput','Input and completion throughput','Messages/s (10-second bins)')]:
        fig,axes=plt.subplots(4,2,figsize=(12,13),sharex=True,sharey=True)
        values=[v for _,data,_ in runs for _,ys in data[metric] for v in ys if math.isfinite(v)];extent=max(max(values)-min(values),1)
        low=min(0,min(values)-.06*extent) if metric=='growth' else 0;high=max(values)+.1*extent;handles=[]
        for ax,(s,data,marks) in zip(axes.flat,runs):
            if metric in ('cpu','memory'):labels=['Consumer '+str(i) for i in range(len(data[metric]))];colors=COLORS
            elif metric=='throughput':labels=['Acknowledged input','Unique completions'];colors=['#7b8794','#2563a6']
            elif metric=='skew':labels=['Snapshot ratio','15-sample mean'];colors=['#a7afb8','#2563a6']
            else:labels=[title];colors=['#2563a6']
            lines=[]
            for (x,y),label,color in zip(data[metric],labels,colors):lines+=ax.plot(x,y,color=color,label=label,lw=1.1)
            if len(lines)>len(handles):handles=lines
            if s['arm']!='keep3':
                ax.axvline(1,color='#777',ls=':',lw=.8)
                left=marks.get('release_requested',marks.get('decision'));right=marks.get('active_verified',marks.get('stable_verified'))
                if left is not None and right is not None:ax.axvspan(left,right,color='#64748b',alpha=.12)
            if metric=='growth':ax.axhline(0,color='#999',lw=.6)
            ax.set(title=LABELS[s['arm']]+' — Run '+str(s['run_number']),xlabel='Evaluation time (minutes)',ylabel=ylabel,xlim=(0,10),ylim=(low,high))
            ax.tick_params(labelbottom=True,labelleft=True);ax.grid(axis='y',alpha=.2)
        fig.suptitle(title+' — 700 messages/s',fontweight='bold',fontsize=16)
        if len(handles)>1:fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.5,.968),ncol=min(len(handles),6),frameon=False,fontsize=9)
        note='1 min warm-up + 10 min evaluation + 2 min drain. Common scales; genuine gaps remain unavailable.'
        if metric=='outstanding':note+='\nDistinct acknowledgment/completion events, including warm-up; this is not broker offset lag.'
        elif metric=='throughput':note+='\nCompletions include warm-up records finishing during evaluation.'
        else:note+='\nDotted line: scheduled intervention. Shading: recorded transition interval; definitions differ by coordination mechanism.'
        fig.text(.07,.015,note,fontsize=9,color='#555');fig.tight_layout(rect=(0,.06,1,.942));figures.append((fig,'four-condition-'+metric))
    fig,axes=plt.subplots(1,2,figsize=(12,5));x=np.arange(2);width=.2
    for index,arm in enumerate(ARMS):
        selected=sorted([s for s,_,_ in runs if s['arm']==arm],key=lambda s:s['run_number'])
        for ax,key in zip(axes,['p99_seconds','unfinished_percent']):
            bars=ax.bar(x+(index-1.5)*width,[s[key] for s in selected],width,label=LABELS[arm],color=COLORS[index])
            for b,s in zip(bars,selected):ax.annotate(f'{s[key]:.1f}',(b.get_x()+b.get_width()/2,b.get_height()),xytext=(0,4),textcoords='offset points',ha='center',fontsize=8)
    for ax,key,label in zip(axes,['p99_seconds','unfinished_percent'],['Completion p99 (seconds)','Unfinished evaluation messages (%)']):
        ax.set_xticks(x);ax.set_xticklabels(['Run 1','Run 2']);ax.set_ylabel(label);ax.set_ylim(0,max(1,max(s[key] for s,_,_ in runs))*1.2);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    h,l=axes[0].get_legend_handles_labels();fig.legend(h,l,loc='upper center',bbox_to_anchor=(.5,.94),ncol=2,frameon=False)
    fig.suptitle('Whole-run completion latency and unfinished work',fontweight='bold')
    fig.text(.07,.02,'P99 describes completed evaluation messages; unfinished work uses the fixed drain cutoff.\nEach trial: 1 min warm-up + 10 min evaluation + 2 min drain. These are separate trials, not within-run before/after p99.',fontsize=9)
    fig.tight_layout(rect=(0,.13,1,.80));figures.append((fig,'four-condition-p99-unfinished'))
    with PdfPages(a.output/'four-condition-metrics.pdf') as pdf:
        for fig,name in figures:
            for ext in ['png','pdf']:fig.savefig(a.output/(name+'.'+ext),dpi=180,facecolor='white')
            pdf.savefig(fig,facecolor='white');plt.close(fig)
    lines=['# Four responses to concentrated ownership','','Eight performance trials used a common verified starting map, all twelve hot partitions on Consumer 2, and 700 aggregate messages/s. Every trial lasted thirteen minutes: one warm-up, ten evaluation and two drain. The second run reversed condition order.','',
        '| Condition | Run | Completion p99 (s) | Unfinished (%) | Useful completions/s | Mean lag | Lag coverage |','|---|---:|---:|---:|---:|---:|---:|']
    for s,_,_ in runs:lines.append(f"| {LABELS[s['arm']]} | {s['run_number']} | {s['p99_seconds']:.2f} | {s['unfinished_percent']:.3f} | {s['useful_throughput']:.2f} | {s['mean_lag']:,.1f} | {s['lag_coverage']*100:.1f}% |")
    lines+=['','P99 is calculated from distinct acknowledged evaluation messages that completed by the drain cutoff; it is not an average of rolling percentiles. Warm-up messages remain queued but are outside that latency cohort. Useful completion throughput includes warm-up work finishing during evaluation.','',
        '| Condition | Run | Requested CPU (core-min) | Requested memory (GiB-min) | Request coverage |','|---|---:|---:|---:|---:|']
    for s,_,_ in runs:
        resource=s['requested_resources_evaluation_and_drain'];lines.append(f"| {LABELS[s['arm']]} | {s['run_number']} | {resource['consumer_container_requested_cpu_seconds_observed']/60:.2f} | {resource['consumer_container_requested_gib_seconds_observed']/60:.2f} | {resource['covered_fraction']*100:.1f}% |")
    lines+=['','Requested resources are integrated over evaluation plus drain; actual process CPU/RSS are retained separately. Missing intervals are excluded, not treated as zero.','',
        '| Condition | Run | Transition (s) | Net additional unfinished messages | Recovery confirmed after decision (s) |','|---|---:|---:|---:|---:|']
    for s,_,_ in runs:
        c=s['intervention_cost']
        if c['action']=='none':continue
        rec=c['recovery_from_decision'][1]['confirmation_after_anchor_seconds'];rec='Not observed during input' if rec is None else f'{rec:.1f}'
        lines.append(f"| {LABELS[s['arm']]} | {s['run_number']} | {c['transition_seconds']:.2f} | {c['net_additional_unfinished']:,} | {rec} |")
    lines+=['','Explicit transition time spans release request through active verification. Native scaling spans the scale decision through ten seconds of complete stable six-owner observations. These are different operational boundaries and must not be interpreted as identical coordination costs. Message accumulation is reconstructed from acknowledgment/completion timestamps and includes warm-up. It is an observed net change, not causal excess relative to a counterfactual.','',
        'Recovery requires total processing backlog at most 100 offsets for thirty consecutive valid seconds with no ownership or offset reset, while production continues. Sensitivity thresholds of 50 and 200 offsets were specified in advance. Per-partition native handover intervals and verified explicit processing pauses are in comparison.json. A cold partition can be naturally idle between records, so its inter-owner message interval is not pure rebalance downtime.','',
        'The normal Kafka arm uses classic cooperative-sticky assignment with per-pod static identities. Other arms use coordinated explicit ownership. This comparison evaluates those implemented responses, including coordination differences. All were scheduled, not selected by an adaptive controller. Two runs and shared-node variability limit generalization. Historical monitoring exports remain unchanged.','',
        '[Full results, definitions and evidence hashes](comparison.json) · [All plots](four-condition-metrics.pdf)','']
    for _,name in figures:lines+=[f'![{name}]({name}.png)','']
    (a.output/'README.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps([{k:s[k] for k in ['arm','run_number','run_id','p99_seconds','unfinished_percent']} for s,_,_ in runs],indent=2))

if __name__=='__main__':main()
