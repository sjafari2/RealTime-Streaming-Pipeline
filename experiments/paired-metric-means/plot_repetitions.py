"""Plot validated repetitions with sample standard deviations and explicit gaps."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/matched-metrics-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator, StrMethodFormatter
import numpy as np
from repetition_statistics import complete_summary, validate_design

REPO = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
ARMS = ['keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6']
LABELS = ['Keep 3', 'Redistribute 3', 'Targeted scale 6', 'Kafka scale 6']
COLORS = ['#2563a6', '#d97b1d', '#168361', '#9653a6']
REPETITIONS = (1, 2)
CONDITIONS = ('smaller', 'larger')
CONDITION_TITLES = ('Smaller starting backlog', 'Larger starting backlog')

METRICS = ['Total backlog', 'Backlog growth', 'Lag skew ratio', 'Message age',
           'Partition arrival rate', 'Consumer capacity', 'Throughput',
           'P99 end-to-end latency', 'Unfinished messages', 'SLO violation rate',
           'Recovery time', 'Resource cost', 'Decision-to-active time']


def read(p):
    return json.loads(Path(p).read_text())


def save(p, d):
    Path(p).write_text(json.dumps(d, indent=2, allow_nan=False)+'\n')


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def load(name, p):
    spec = importlib.util.spec_from_file_location(name, p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


windows = load('windows', REPO/'experiments/four-condition-20260929/analyze_action_windows.py')
means = load('means', REPO/'experiments/twelve-hot600-20261104/plot_repeat_means.py')


def age_series(ack, done, start, end):
    """Mean age of acknowledged work still awaiting completion, including warm-up."""
    admitted = sorted((a, p) for p, a in ack.values())
    settled = sorted((max(ack[k][1], v[0]), ack[k][0]) for k,v in done.items())
    def cumulative(rows):
        return np.asarray([x[0] for x in rows]), np.r_[0., np.cumsum([x[1]-start for x in rows])]
    at, ap = cumulative(admitted)
    dt, dp = cumulative(settled)
    output = []
    for seconds in range(0, round(end-start)+1, 2):
        t = start+seconds
        na, nd = np.searchsorted(at,t,side='right'), np.searchsorted(dt,t,side='right')
        pending = int(na-nd)
        if pending < 0:
            raise ValueError('Negative pending population')
        age = seconds-float(ap[na]-dp[nd])/pending if pending else None
        if age is not None and age < -1e-6:
            raise ValueError('Negative age')
        output.append([seconds, max(0.,age) if age is not None else None])
    return output


def resource_cost(source, metric, divisor):
    records = [r for r in source['process_resources']
               if r['pod'].startswith('consumer-') and r['metric']==metric]
    if not records:
        return None, None
    values = [r['windows']['evaluation_and_drain'] for r in records]
    integral = sum(v['observed_integral'] for v in values)/divisor/60
    coverage = sum(v['covered_seconds'] for v in values)/sum(v['requested_window_seconds'] for v in values)
    return integral, coverage


def collect(comparison, out):
    source = read(comparison)
    validate_design(source['runs'], CONDITIONS, ARMS, REPETITIONS)
    cache=out/'parts'
    cache.mkdir(parents=True,exist_ok=True)
    rows=[]
    for s in source['runs']:
        signature=hashlib.sha256(json.dumps(s,sort_keys=True).encode()).hexdigest()
        cached=cache/(s['run_id']+'.json')
        if cached.exists():
            row=read(cached)
            assert row['comparison_row_sha256']==signature
            rows.append(row)
            continue
        print('Reconstructing',s['run_id'],flush=True)
        directory=REPO/s['raw_evidence']
        for filename in ('manifest.json','lag-summary.json','prometheus.json'):
            assert sha(directory/filename)==s['evidence_hashes'][filename]
        m=read(directory/'manifest.json')
        start,end,cutoff=(m[k] for k in ('evaluation_start_epoch','producer_end_epoch','drain_end_epoch'))
        row=dict(run_id=s['run_id'],condition=s['backlog_condition'],arm=s['arm'],run=s['run_number'],
                 comparison_row_sha256=signature,evidence_hashes=s['evidence_hashes'],
                 activation=s['activation'], whole_p99=s['p99_seconds'],
                 unfinished_percent=s['unfinished_percent'],unfinished=s['unfinished'],
                 slo_percent=100*s['deadline_miss_fraction'],throughput_run=s['useful_throughput'],
                 recovery=s['recovery_common_decision']['confirmation_after_anchor_seconds'],
                 decision_to_active=None if s['arm']=='keep3' else s['activation']['epoch']-s['intervention_cost']['decision_epoch'])
        lag=read(directory/'lag-summary.json')
        for key,field in [('backlog','window_mean_backlog'),('growth','window_growth_offsets_per_second'),('skew','window_mean_skew')]:
            row[key]=[[v['timestamp']-start,v.get(field) if v['valid'] else None]
                      for v in lag['snapshots'] if start<=v['timestamp']<end]
        rawvalid=[v for v in lag['snapshots'] if start<=v['timestamp']<end and v['valid']]
        row['backlog_run']=float(np.mean([v['total_lag'] for v in rawvalid]))
        row['skew_run']=float(np.mean([v['skew'] for v in rawvalid]))
        growth=[v['growth_offsets_per_second'] for v in rawvalid if v.get('growth_offsets_per_second') is not None]
        row['growth_run']=float(np.mean(growth)) if growth else None
        ack,done,event_hashes=windows.load_messages(directory,m,s['intervention_cost']['event_file_sha256'])
        lat=[v[0]-ack[k][0] for k,v in done.items() if start<=ack[k][0]<end and v[0]<=cutoff]
        assert len(lat)==s['completed']
        assert math.isclose(windows.p99(lat),s['p99_seconds'],abs_tol=1e-8)
        assert sum(start<=v[0]<end for v in ack.values())==s['admitted']
        row['age']=age_series(ack,done,start,end)
        row['age_at_action']=dict(row['age'])[60]
        series=windows.build_series(ack,done,start,end)
        calculated=[windows.window_metrics(series,start+t,start+t+30) for t in range(0,600,30)]
        row['throughput']=[[t+30,w['completion_throughput_per_second']] for t,w in zip(range(0,600,30),calculated)]
        row['p99']=[[t+30,w['completion_window_p99_seconds']] for t,w in zip(range(0,600,30),calculated)]
        assert math.isclose(sum(v*30 for _,v in row['throughput'])/600,s['useful_throughput'],rel_tol=1e-9)
        row['message_hashes']=event_hashes
        row['cpu_cost'],row['cpu_coverage']=resource_cost(s,'consumer_cpu_percent',100)
        row['memory_cost'],row['memory_coverage']=resource_cost(s,'consumer_memory_bytes',2**30)
        b=s.get('arrival_and_capacity',{}).get('before',{})
        row['arrival_status']=b.get('status')
        row['partition_rates']=b.get('measured_partition_rates',{}).get('partitions')
        row['capacity']={pod:v['calibrated_capacity'] for pod,v in b.get('consumers',{}).items()}
        row['arrival_reason']=b.get('reason')
        save(cached,row)
        rows.append(row)
        del ack,done,series,lat
    return rows


def enrich(rows, comparison):
    """Separate counter availability from ownership validity and account for startup."""
    sys.path.insert(0,str(REPO/'experiments/signal-outcome-20261021'))
    from build_capacity import CounterData, partition_arrivals
    from evidence_io import event_paths, open_events
    sources={s['run_id']:s for s in read(comparison)['runs']}
    for row in rows:
        source=sources[row['run_id']]
        directory=REPO/source['raw_evidence']
        manifest=read(directory/'manifest.json')
        start,end=manifest['evaluation_start_epoch'],manifest['drain_end_epoch']
        starts={}
        for path in event_paths(directory):
            final=read(path.with_name('final.json'))
            if final['role']=='consumer':
                with open_events(path) as stream:
                    first=json.loads(next(stream))
                assert first['event']=='started' and final['pod'] not in starts
                starts[final['pod']]=first['timestamp']
        for name,key in [('consumer_cpu_percent','cpu_coverage'),('consumer_memory_bytes','memory_coverage')]:
            records=[r for r in source['process_resources'] if r['pod'] in starts and r['metric']==name]
            covered=sum(r['windows']['evaluation_and_drain']['covered_seconds'] for r in records)
            expected=sum(end-max(start,starts[r['pod']]) for r in records)
            row[key]=covered/expected
            assert 0<=row[key]<=1.001
        # Arrival counters do not require a complete lag window. The existing
        # assignment diagnostic rejected both when only its lag check failed.
        if row['partition_rates'] is None:
            try:
                result=partition_arrivals(CounterData(read(directory/'prometheus.json'),manifest),manifest,30,60)
                row['partition_rates']=result['partitions']
                row['arrival_status']='available from fresh producer counters'
                row['arrival_evidence']=result['producer_intervals']
            except ValueError as exc:
                row['arrival_reason']=str(exc)
    return rows


def group_runs(rows,c,a):
    values=sorted([r for r in rows if r['condition']==c and r['arm']==a],key=lambda r:r['run'])
    if [r['run'] for r in values] != list(REPETITIONS):
        raise ValueError('Missing or duplicate repetition in plot group')
    return values


def average(values):
    return complete_summary(values)['mean']


def repeated_series(runs, metric, step):
    aligned = [means.align(run[metric], step) for run in runs]
    grid = aligned[0][0]
    values = np.asarray([value for _, value in aligned])
    valid = np.isfinite(values).all(axis=0)
    mean = np.where(valid, values.mean(axis=0), np.nan)
    deviation = np.where(valid, values.std(axis=0, ddof=1), np.nan) if len(runs) > 1 else np.zeros(len(grid))
    return grid, mean, mean-deviation, mean+deviation


def setup(n,title,legend=True):
    height=2.45*n+1.55
    fig,axes=plt.subplots(n,2,figsize=(11.6,height),sharey='row',squeeze=False)
    fig.suptitle(title,fontsize=15,y=.99,fontweight='bold')
    for j,c in enumerate(CONDITION_TITLES):
        axes[0,j].set_title(c,fontsize=12,pad=10)
    if legend:
        fig.legend(handles=[Line2D([0],[0],color=c,lw=2,label=l) for c,l in zip(COLORS,LABELS)],
                   loc='upper center',bbox_to_anchor=(.5,1-.48/height),ncol=4,frameon=False,fontsize=9)
    fig.subplots_adjust(left=.09,right=.98,top=1-(1.35 if legend else .9)/height,bottom=.90/height,hspace=.38,wspace=.23)
    for ax in axes.flat:
        ax.grid(alpha=.16)
        ax.tick_params(labelleft=True,labelsize=9)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:g}'))
    return fig,axes


def finish(fig,path,note=''):
    if note:
        fig.text(.5,.025,note,ha='center',va='bottom',fontsize=8)
    for ext in ('png','pdf','svg'):
        fig.savefig(path.with_suffix('.'+ext),dpi=180,facecolor='white')
    plt.close(fig)


def time_figure(rows,panels,title,out):
    fig,axes=setup(len(panels),title)
    for col,condition in enumerate(CONDITIONS):
        for i,(metric,label,step) in enumerate(panels):
            ax=axes[i,col]
            for arm,color in zip(ARMS,COLORS):
                rr=group_runs(rows,condition,arm)
                x,y,lo,hi=repeated_series(rr,metric,step)
                ax.plot(x/60,y,color=color,lw=1.5)
                ax.fill_between(x/60,lo,hi,color=color,alpha=.10,lw=0)
                if arm!='keep3':
                    for r in rr:ax.axvline(r['activation']['after_evaluation_seconds']/60,color=color,ls=':',lw=.65,alpha=.65)
            ax.axvline(1,color='#555555',ls=':',lw=.8)
            ax.set(xlim=(0,10),ylabel=label)
            if metric=='growth': ax.axhline(0,color='#777777',lw=.6)
            else: pass
            if metric=='throughput':ax.axhline(600,color='#777777',ls='--',lw=.7)
            if i==len(panels)-1: ax.set_xlabel('Evaluation time (min)',fontsize=10)
    for i,(metric,_,step) in enumerate(panels):
        vv=[]
        for c in CONDITIONS:
            for a in ARMS:
                _,_,lo,hi=repeated_series(group_runs(rows,c,a),metric,step)
                vv.extend(lo[np.isfinite(lo)].tolist()+hi[np.isfinite(hi)].tolist())
        if vv:
            low=min(0,min(vv))*1.06 if metric=='growth' else 0
            high=max(vv)*1.06 if max(vv)>0 else 1
            axes[i,0].set_ylim(low,high)
        if metric in ('backlog','growth','throughput'):
            for ax in axes[i]:ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
    n = len(REPETITIONS)
    note = (f'Lines: mean of {n} runs. Bands: mean ± sample SD.' if n > 1 else 'One run per response. No standard deviation estimated.')
    note += '\nDotted lines: gray = request at minute 1, colors = each confirmed activation. Missing values remain gaps.'
    if any(metric == 'throughput' for metric, _, _ in panels):
        note += '\nDashed line: input at 600 msg/s. Throughput and window p99 use 30-second intervals. Drain is excluded.'
    finish(fig,out,note)


def point_figure(rows,panels,title,out):
    fig,axes=setup(len(panels),title,legend=False)
    for j,condition in enumerate(CONDITIONS):
        for i,(metric,label) in enumerate(panels):
            ax=axes[i,j]
            ax.set_ylabel(label)
            for k,(arm,color) in enumerate(zip(ARMS,COLORS)):
                values=[r[metric] for r in group_runs(rows,condition,arm)]
                mean=average(values)
                if mean is not None:
                    sd = complete_summary(values)['sample_sd']
                    if sd is not None:
                        ax.errorbar(k,mean,yerr=sd,fmt='none',color=color,capsize=3,lw=1.2)
                    ax.scatter(np.linspace(k-.10,k+.10,len(values)),values,facecolor='none',edgecolor=color,s=20,zorder=2)
                    ax.scatter([k],[mean],color=color,s=36,zorder=3,clip_on=False)
                else:
                    valid=[v for v in values if v is not None]
                    if valid:ax.scatter([k]*len(valid),valid,facecolor='none',edgecolor=color,s=36,zorder=3,clip_on=False)
                    labeltext=('No action' if metric=='decision_to_active' and arm=='keep3' else
                               str(len(valid))+'/'+str(len(REPETITIONS))+' confirmed' if metric=='recovery' else 'Unavailable')
                    ax.text(k,.93,labeltext,transform=ax.get_xaxis_transform(),ha='center',va='top',fontsize=8,color=color)
            ax.set_xlim(-.45,3.45)
            ax.set_xticks(range(4))
            ax.set_xticklabels(['Keep 3','Redistribute 3','Targeted\nscale 6','Kafka\nscale 6'],fontsize=8.5)
    for i,(metric,_) in enumerate(panels):
        vv=[r[metric] for r in rows if r[metric] is not None]
        for c in CONDITIONS:
            for a in ARMS:
                result = complete_summary([r[metric] for r in group_runs(rows,c,a)])
                if result['mean'] is not None:
                    vv.append(result['mean'] + (result['sample_sd'] or 0))
        axes[i,0].set_ylim(0,max(vv)*1.12 if vv and max(vv)>0 else 1)
    note = f'Filled points: means of {len(REPETITIONS)} runs. Open points: individual runs.'
    note += '\nWhiskers: sample SD when all outcomes are observed.'
    if any(metric == 'recovery' for metric, _ in panels):
        note += ' Unconfirmed recovery is not assigned a numerical value.'
    if any(metric in ('cpu_cost', 'memory_cost') for metric, _ in panels):
        coverage = [r[key] for r in rows for key in ('cpu_coverage', 'memory_coverage') if r.get(key) is not None]
        note += '\nCosts integrate available CPU and RSS observations over evaluation and drain.'
        if coverage:
            note += f' Coverage: {100*min(coverage):.1f}–{100*max(coverage):.1f}%.'
    finish(fig,out,note)


def candidate_figure(rows,out,capacity_audit=None):
    fig,axes=setup(2,'Runtime control signals: candidate responses')
    capacity_limits=[]
    for j,condition in enumerate(CONDITIONS):
        for arm,color in zip(ARMS,COLORS):
            rr=group_runs(rows,condition,arm)
            if all(r['partition_rates'] is not None for r in rr):
                keys=sorted(rr[0]['partition_rates'],key=lambda k:int(k.split('/')[-1]))
                values=[average([r['partition_rates'][k] for r in rr]) for k in keys]
                x = [int(k.split('/')[-1]) for k in keys]
                axes[0,j].plot(x,values,color=color,lw=1,alpha=.8)
                deviations = [complete_summary([r['partition_rates'][k] for r in rr])['sample_sd'] for k in keys]
                if all(v is not None for v in deviations):
                    axes[0,j].fill_between(x,np.asarray(values)-deviations,np.asarray(values)+deviations,color=color,alpha=.10,lw=0)
        caps={}
        for r in rows:
            if r['condition']==condition:
                for pod,value in r['capacity'].items():
                    assert pod not in caps or caps[pod]==value
                    caps[pod]=value
        ranges={}
        if capacity_audit is not None:
            caps={}
            for consumer in range(6):
                pod='consumer-sts-'+str(consumer)
                windows=[w for r in capacity_audit['records']
                         if r['source_kind']=='mitigation_trial' and r['workload']==condition and r['pod']==pod
                         for w in r['windows'] if w['eligible']]
                if windows:
                    caps[pod]=sum(w['completions'] for w in windows)/sum(w['observed_seconds'] for w in windows)
                    ranges[pod]=(min(w['effective_rate'] for w in windows),max(w['effective_rate'] for w in windows))
            axes[1,j].text(.5,.97,'Retrospective busy-period estimates',transform=axes[1,j].transAxes,
                           ha='center',va='top',fontsize=9)
        x=list(range(6));y=[caps.get('consumer-sts-'+str(c),np.nan) for c in x]
        axes[1,j].scatter(x,y,color='#446986',s=40,zorder=3)
        for pod,(low,high) in ranges.items():
            consumer=int(pod.rsplit('-',1)[-1]); value=caps[pod]
            axes[1,j].errorbar(consumer,value,yerr=[[value-low],[high-value]],color='#446986',capsize=3,lw=1)
        capacity_limits.extend(caps.values())
        capacity_limits.extend(high for _,high in ranges.values())
        axes[1,j].set_xticks(x)
        axes[1,j].set_xlabel('Consumer index')
        for c in x:
            if math.isnan(y[c]):axes[1,j].text(c,.06,'No eligible\ninterval' if capacity_audit else 'Not\nmeasured',transform=axes[1,j].get_xaxis_transform(),ha='center',fontsize=8)
        axes[0,j].set(xlabel='Partition index',ylabel='Arrival (msg/s)',xlim=(-1,60))
        axes[1,j].set(ylabel='Capacity (msg/s)',xlim=(-.5,5.5))
    arrivals=[v for r in rows for v in (r['partition_rates'] or {}).values() if v is not None]
    capacities=capacity_limits
    if CONDITIONS == ('smaller', 'larger'):
        axes[0,0].set_ylim(0,46)
        axes[1,0].set_ylim(0,750)
    else:
        axes[0,0].set_ylim(0,max(arrivals)*1.15 if arrivals else 1)
        axes[1,0].set_ylim(0,max(capacities)*1.20 if capacities else 1)
    if not capacities:
        for ax in axes[1]:
            ax.set_yticks([])
            ax.text(.5,.60,'Matching busy calibration unavailable',transform=ax.transAxes,
                    ha='center',fontsize=10)
    note = 'Arrival lines: equal-weight run means, with sample SD bands when repeated.'
    note += ('\nCapacity points: retrospective eligible-interval rates. Capacity whiskers show interval ranges, not run SD.' if capacity_audit else '\nCapacity points: independent calibration references where available.')
    finish(fig,out,note)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--comparison',type=Path,default=REPO/'results/four-condition-20261027-003002-analysis/comparison.json')
    p.add_argument('--plots-only',action='store_true')
    p.add_argument('--run-numbers',type=int,nargs='+',default=[1,2])
    p.add_argument('--capacity-audit',type=Path,help='Retrospective trial-derived rates, kept separate from calibrated capacity')
    p.add_argument('--dataset',choices=['backlog','matched-skew'],default='backlog')
    args=p.parse_args();out=args.output;out.mkdir(parents=True,exist_ok=True)
    global CONDITIONS, CONDITION_TITLES, REPETITIONS
    REPETITIONS = tuple(sorted(args.run_numbers))
    if not REPETITIONS or len(set(REPETITIONS)) != len(REPETITIONS) or min(REPETITIONS) < 1:
        p.error('Use distinct positive run numbers')
    if args.dataset=='matched-skew':
        CONDITIONS=('hot12','hot4')
        CONDITION_TITLES=('12 high-traffic partitions','4 high-traffic partitions')
    prefix='backlog-mean' if args.dataset=='backlog' else 'skew-mean'
    capacity_audit=read(args.capacity_audit) if args.capacity_audit else None
    if capacity_audit is not None:
        assert args.dataset=='matched-skew'
        assert capacity_audit['comparison_sha256']==sha(args.comparison)
    if args.plots_only:
        cached=read(out/'plot-data.json')
        assert cached['comparison_sha256']==sha(args.comparison)
        rows=cached['rows']
    else:
        rows=enrich(collect(args.comparison,out),args.comparison)
    save(out/'plot-data.json',dict(metrics=METRICS,rows=rows,comparison_sha256=sha(args.comparison),
         averaging=f'Equal-weight means of {len(REPETITIONS)} run-level values and sample SD. Time-series means require all observations. No pooling of message latencies.',
         age='Mean elapsed time since creation among all messages acknowledged but not completed at each observation, including warm-up. Empty sets are unavailable.',
         capacity=('The capacity panel shows retrospective busy-period estimates separately for each workload condition. These are not independent calibration or means of run-level capacities.' if capacity_audit else
                   'Matching original-consumer calibration estimates are fixed references, not new independent estimates from each trial.'),
         capacity_audit_sha256=sha(args.capacity_audit) if args.capacity_audit else None,
         resource_cost='Sum of observed consumer CPU and RSS integrals over evaluation and drain. Missing intervals are omitted, not filled with zero.'))
    summaries=[]
    for c in CONDITIONS:
        for a in ARMS:
            rr=group_runs(rows,c,a)
            record=dict(condition=c,arm=a,runs=[r['run_id'] for r in rr])
            for key in ('whole_p99','unfinished','unfinished_percent','slo_percent','throughput_run','recovery','decision_to_active','cpu_cost','memory_cost','backlog_run','growth_run','skew_run','age_at_action'):
                record[key]=complete_summary([r[key] for r in rr])
            record['cpu_coverage']=[r['cpu_coverage'] for r in rr]
            record['memory_coverage']=[r['memory_coverage'] for r in rr]
            summaries.append(record)
    save(out/'run-statistics.json',summaries)
    time_figure(rows,[('backlog','Backlog (offsets)',2),('growth','Growth (offsets/s)',2),('skew','Lag skew',2),('age','Mean age (s)',2)],
                'Runtime control signals: intervention need',out/(prefix+'-control'))
    time_figure(rows,[('throughput','Throughput (msg/s)',30),('p99','Window p99 (s)',30)],
                'Performance evaluation metrics',out/(prefix+'-performance'))
    candidate_figure(rows,out/(prefix+'-candidates'),capacity_audit)
    point_figure(rows,[('whole_p99','P99 latency (s)'),('unfinished_percent','Unfinished (%)'),('slo_percent','SLO violations (%)'),('recovery','Recovery (s)')],
                 'Performance evaluation metrics: run outcomes',out/(prefix+'-outcomes'))
    point_figure(rows,[('cpu_cost','CPU (core-min)'),('memory_cost','Memory (GiB-min)'),('decision_to_active','Decision–active (s)')],
                 'Cost metrics',out/(prefix+'-costs'))
    save(out/'validation.json',dict(trials=len(rows),pairs=len(summaries),metrics=METRICS,
         checks=['Raw manifest, lag and monitoring hashes match saved evidence',
                 'Message hashes checked and duplicate completions removed',
                 'Evaluation cohort sizes and run p99 reconcile with recorded results',
                 '30-second throughput integrates to recorded evaluation throughput',
                 'Missing observations and unconfirmed recovery are not zero'],
         limitations=['Partition-rate availability is assessed independently of lag-window availability',
                      ('Capacity estimates are retrospective and shown only where busy, stable trial intervals pass the existing screen' if capacity_audit else
                       'Capacity is shown only where matching busy-calibration evidence exists'),
                      'Resource costs cover observed intervals only',
                      'Unconfirmed recovery is reported separately and prevents a complete-group recovery mean']))
    print('Complete',out,flush=True)


if __name__=='__main__':main()
