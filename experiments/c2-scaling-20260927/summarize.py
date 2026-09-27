"""Summarize the controlled Consumer 2 scale-and-redistribute comparison."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys
os.environ.setdefault('MPLCONFIGDIR','/private/tmp/c2-scaling-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[2]))
sys.path.insert(0,str(ROOT/'python-scripts'))
from analyze_stability import window
from analyze_execution import resource_integral
spec=importlib.util.spec_from_file_location('ownership_figures',ROOT/'experiments/hot-ownership-20260927/summarize.py')
old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
COLORS=('#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b')


def read(path,name):return json.loads((path/name).read_text())


def load(row, resource_observations):
    p=Path(row['directory']);m=read(p,'manifest.json');o=read(p,'outcome-summary.json');lag=read(p,'lag-summary.json')
    audit=read(p,'measurement-audit.json');validation=read(p,'handoff-validation.json')
    assert read(p,'runner-status.json')['status']=='complete' and validation['status']=='passed'
    assert not o['validity_failures'] and not audit['issues']
    cfg=m['config'];assert float(cfg['TARGET_RATE'])*3==700
    assert all(cfg[k]==v for k,v in {'NUM_PARTITIONS':'60','WARMUP_SECONDS':'60','EXP_DURATION_SEC':'360',
                                   'DRAIN_SECONDS':'120','APP_CPU_ITERATIONS':'2000','APP_DELAY_MS':'0'}.items())
    initial=m['explicit_start_verification'];assert initial['verified']
    assert all(x['owner']=='consumer-sts-2' for x in initial['ownership'] if x['partition']<12)
    assert len(m['consumer_pods'])==3
    transitions=[]
    if row['arm']=='scale6':
        assert m['intervention']['action']=='scale_redistribute'
        transitions=[json.loads(line) for line in (p/'explicit-handoff-events.jsonl').read_text().splitlines()]
    else:assert m['intervention']['action']=='none'
    start,end=m['evaluation_start_epoch'],m['producer_end_epoch']
    markers={e['event']:(e['timestamp']-start)/60 for e in transitions}
    resource=resource_observations or [json.loads(line) for line in (p/'resource-history.jsonl').read_text().splitlines()]
    s=dict(arm=row['arm'],run_number=row['run_number'],seed=row['seed'],run_id=m['run_id'],
        raw_evidence='results/'+m['run_id'],admitted=o['admitted_evaluation_cohort'],completed=o['completed_by_drain'],
        unfinished=o['incomplete_by_drain'],unfinished_percent=100*o['incomplete_fraction'],
        p99_seconds=o['admitted_cohort_completion_p99_seconds'],mean_completion_seconds=o['admitted_cohort_completion_mean_seconds'],
        useful_throughput=o['useful_throughput_per_second'],admitted_rate=o['admitted_messages_per_second'],
        deadline_miss_fraction=o['deadline_miss_fraction'],deadline_seconds=float(cfg['SLO_THRESHOLD_MS'])/1000,
        mean_lag=lag['time_weighted_mean_lag'],peak_lag=lag['peak_sampled_lag'],lag_coverage=lag['covered_fraction'],
        mean_snapshot_skew=statistics.mean(x['skew'] for x in lag['snapshots'] if x['valid']),
        growth_windows={'whole_evaluation':window(lag['snapshots'],start,end),
                        'last_two_minutes':window(lag['snapshots'],end-120,end)},
        process_resources=audit['process_resources'],initial_resources=m['initial_consumer_resources'],
        requested_resources_evaluation_and_drain=resource_integral(resource,start,m['drain_end_epoch'],15),
        transition_times_evaluation_minutes=markers,validation=validation,config=cfg,
        observed_consumer_pods=read(p,'execution-summary.json')['observed_consumer_pods'],
        resource_accounting_source='independent_two_second_observer' if resource_observations else 'coordinator_samples',
        evidence_hashes={name:hashlib.sha256((p/name).read_bytes()).hexdigest() for name in
           ('manifest.json','pipeline-configmap.yaml','outcome-summary.json','lag-summary.json','measurement-audit.json',
            'execution-summary.json','handoff-validation.json','prometheus.json')})
    return p,m,lag,s


def curves(run):
    p,m,lag,s=run;x,total,_,growth=old.lag_lines(m,lag)
    data={'lag':[(x,total)],'growth':[(x,growth)]}
    sx,raw,smooth=old.skew_lines(m,lag);data['skew']=[(sx,raw),(sx,smooth)]
    series=read(p,'prometheus.json')['data']['result']
    count=6 if s['arm']=='scale6' else 3
    for key,name,div in [('cpu','consumer_cpu_percent',100),('memory','consumer_memory_bytes',1024**2)]:
        data[key]=[old.resource_line(series,name,'consumer-sts-'+str(i),m['evaluation_start_epoch'],m['producer_end_epoch'],div) for i in range(count)]
    tx,ys=old.throughput_bins(p,m['evaluation_start_epoch'],m['producer_end_epoch']);data['throughput']=[(tx,y) for y in ys]
    return data


def marker(ax,s):
    if s['arm']!='scale6':return
    ax.axvline(1,color='#555555',ls=':',lw=1)
    marks=s['transition_times_evaluation_minutes']
    if marks:
        ax.axvspan(marks['release_requested'],marks['active_verified'],color='#64748b',alpha=.13,zorder=0)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('campaign',type=Path);parser.add_argument('output',type=Path)
    a=parser.parse_args();campaign=read(a.campaign,'campaign-status.json')
    assert campaign['status']=='complete' and campaign.get('restoration')=='verified' and len(campaign['runs'])==4
    observer=a.campaign/'resource-observations.jsonl'
    resources=[json.loads(line) for line in observer.read_text().splitlines()] if observer.exists() else []
    runs=sorted([load(x,resources) for x in campaign['runs']],key=lambda r:(r[3]['run_number'],r[3]['arm']))
    assert [(s['run_number'],s['arm']) for _,_,_,s in runs]==[(1,'keep3'),(1,'scale6'),(2,'keep3'),(2,'scale6')]
    assert len({m['config']['TOPIC_TITLE'] for _,m,_,_ in runs})==4
    assert all(m['placement_reference']==runs[0][1]['placement_reference'] for _,m,_,_ in runs)
    data=[curves(r) for r in runs];a.output.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    figures=[]
    lag_top=max(v for d in data for _,ys in d['lag'] for v in ys if math.isfinite(v))*1.08
    fig,axes=plt.subplots(1,2,figsize=(12,4.8),sharex=True,sharey=True)
    for index,ax in enumerate(axes):
        for offset,color,label in [(0,COLORS[0],'Keep 3'),(1,COLORS[1],'Scale 3 to 6 + redistribution')]:
            i=index*2+offset;x,y=data[i]['lag'][0];ax.plot(x,y,color=color,label=label,lw=1.6)
        marker(ax,runs[index*2+1][3]);ax.set(title='Run '+str(index+1),xlabel='Evaluation time (minutes)',ylabel='Total lag (offsets)',xlim=(0,5),ylim=(0,lag_top));ax.grid(axis='y',alpha=.2)
        ax.yaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter('{x:,.0f}'))
    axes[0].legend(frameon=False,fontsize=9)
    fig.suptitle('Scaling from concentrated Consumer 2 ownership — 700 messages/s',fontweight='bold')
    fig.text(.07,.02,'1 min warm-up + 5 min evaluation + 2 min drain. Dotted line: scheduled scale request. Shading: handoff coordination.\nEvaluation only; gaps remain unavailable. Both runs start with all twelve hot partitions on Consumer 2.',fontsize=9,color='#555555')
    fig.tight_layout(rect=(0,.11,1,.94));figures.append((fig,'c2-scaling-lag'))
    names={'cpu':('Consumer CPU usage','Process CPU (cores)'), 'memory':('Consumer memory usage','Process RSS (MiB)'),
           'throughput':('Input and completion throughput','Messages/s (10-second bins)'),
           'growth':('Processing-backlog growth','Backlog growth (offsets/s)'), 'skew':('Partition lag skew','Maximum / mean partition lag')}
    for metric,(title,ylabel) in names.items():
        fig,axes=plt.subplots(2,2,figsize=(12,8.3),sharex=True,sharey=True)
        values=[v for d in data for _,ys in d[metric] for v in ys if math.isfinite(v)]
        extent=max(max(values)-min(values),1)
        bounds=(min(0,min(values)-.08*extent) if metric=='growth' else 0,max(values)+.1*extent)
        handles=[]
        for i,(ax,run,d) in enumerate(zip(axes.flat,runs,data)):
            s=run[3]
            if metric in ('cpu','memory'):labels=['Consumer '+str(j) for j in range(len(d[metric]))];colors=COLORS
            elif metric=='throughput':labels=['Acknowledged input','Unique completions'];colors=('#64748b','#176b93')
            elif metric=='skew':labels=['Snapshot ratio','15-sample rolling mean'];colors=('#94a3b8','#176b93')
            else:labels=['30-second growth'];colors=('#176b93',)
            plotted=[]
            for (x,y),label,color in zip(d[metric],labels,colors):plotted+=ax.plot(x,y,label=label,color=color,lw=.7 if metric=='skew' and label=='Snapshot ratio' else 1.3)
            if len(plotted)>len(handles):handles=plotted
            marker(ax,s)
            if metric=='growth':ax.axhline(0,color='#888888',lw=.6)
            ax.set(title=('Keep 3' if s['arm']=='keep3' else 'Scale 3 to 6 + redistribution')+' — Run '+str(s['run_number']),xlabel='Evaluation time (minutes)',ylabel=ylabel,xlim=(0,5))
            ax.tick_params(labelleft=True,labelbottom=True);ax.grid(axis='y',alpha=.2)
            ax.set_ylim(*bounds)
        fig.suptitle(title+' — 700 messages/s',fontweight='bold',fontsize=15)
        fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.5,.95),ncol=min(len(handles),6),frameon=False,fontsize=9)
        note='Common scales. Dotted line: scheduled scale request; shading: handoff coordination. Gaps remain unavailable.'
        if metric=='throughput':note+='\nCompletions include warm-up records finishing during evaluation.'
        if metric in ('cpu','memory'):note+='\nProcess measurements only. New-consumer traces begin when those processes are observed.'
        if metric=='skew':note+='\nInterpret relative skew together with absolute lag and growth; a high ratio alone does not imply a large backlog.'
        fig.text(.065,.025,note,fontsize=9,color='#555555');fig.tight_layout(rect=(0,.095,1,.91));figures.append((fig,'c2-scaling-'+metric))
    with PdfPages(a.output/'c2-scaling-metrics.pdf') as pdf:
        for fig,name in figures:
            for suffix in ('png','pdf'):fig.savefig(a.output/(name+'.'+suffix),dpi=180,facecolor='white')
            pdf.savefig(fig,facecolor='white')
    summaries=[r[3] for r in runs]
    technical=dict(campaign['validation']);technical.pop('directory',None);technical['raw_evidence']='results/'+technical['run_id']
    result=dict(execution_revision=campaign['code_commit'],resource_observer_sha256=hashlib.sha256(observer.read_bytes()).hexdigest() if observer.exists() else None,technical_validation=technical,runs=summaries,
        limitations=['Two trials per condition on shared machines; finite observation.',
        'Scaling and predefined redistribution are combined; this does not isolate the benefit of extra replicas.',
        'The global handoff barrier, replica startup and observed placement affect outcomes.',
        'P99 includes completed evaluation messages only; unfinished work is reported separately.',
        'No adaptive controller, Kafka custom group assignor or fault-tolerant exactly-once effects are claimed.'])
    (a.output/'comparison.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    lines=['# Scaling from concentrated Consumer 2 ownership','','Four performance trials used the same 700 msg/s aggregate 80/20 input and verified starting ownership. Each lasted eight minutes (one warm-up, five evaluation, two drain). Every run started with twelve hot partitions on Consumer 2. Scaling requested six replicas at evaluation +60 seconds and redistributed two hot and eight cold partitions to each. The short technical validation is separate from the performance count.','',
    '| Run | Condition | Completion p99 (s) | Unfinished | Mean lag | Peak lag | Useful throughput (msg/s) |','|---|---|---:|---:|---:|---:|---:|']
    for s in summaries:lines.append(f"| {s['run_number']} | {'Keep 3' if s['arm']=='keep3' else 'Scale 3 to 6 + redistribution'} | {s['p99_seconds']:.3f} | {s['unfinished_percent']:.3f}% | {s['mean_lag']:.1f} | {s['peak_lag']:,.0f} | {s['useful_throughput']:.2f} |")
    lines+=['','P99 is the nearest-rank percentile of completed evaluation messages through application completion, before commit acknowledgment. It is not an average of rolling p99 values. Mean lag is time-weighted over valid intervals. Missing observations and ownership transitions break plotted lines. Unfinished messages are counted at the fixed drain cutoff; warm-up messages remain queued but are excluded from that cohort.','',
    'The comparison tests added replicas and a predeclared assignment change together. A redistribution-only arm would be needed to isolate the value of extra capacity. Original pods, resources and initial owners were checked against one fixed reference; added pods were not pinned. Two runs per condition do not establish long-term stability or remove shared-machine variability.','',
    'Full metric definitions, configuration, process CPU/RSS, deadline outcomes, growth windows, request integrals, coverage, transition timing, identity checks and source-evidence hashes are retained in comparison.json. Large raw evidence remains in the separate results storage.','',
    '[All comparison plots](c2-scaling-metrics.pdf)','']
    for _,name in figures:lines += [f'![{name}]({name}.png)','']
    (a.output/'README.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps([{k:s[k] for k in ('arm','run_number','run_id','p99_seconds','unfinished_percent','mean_lag','peak_lag','lag_coverage')} for s in summaries],indent=2))


if __name__=='__main__':main()
