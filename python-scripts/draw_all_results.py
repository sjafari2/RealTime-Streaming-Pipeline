"""Draw all recorded comparisons without averaging away unfavorable repetitions."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from analyze_lag import signals
from evidence_io import event_paths, open_events

ROOT=Path(__file__).resolve().parents[1]
COLORS={'none':'#17668a','scale':'#c65a28'}
LABELS={'none':'Keep 3','scale':'Scale to 6'}
NAMES={'low-input':'Balanced, lower target rate','sustained':'Balanced pressure, 12 min',
       'short':'Balanced pressure, 5 min','isolated':'80% to one partition, matching not required',
       '8020':'80% across 12 partitions, matching verified',
       'single-partition':'80% to one partition, matching verified'}


def event_rates(directory, manifest, source_hashes, width=30):
    start,end=manifest['evaluation_start_epoch'],manifest['producer_end_epoch']
    bins=np.arange(start,end+width*.5,width)
    if bins[-1]<end:bins=np.append(bins,end)
    result={}
    for role,event,field in [('consumer','completed','completion_timestamp'),('producer','acknowledged','timestamp')]:
        earliest={}
        for path in event_paths(directory/role):
            with open_events(path) as stream:
                digest=hashlib.sha256()
                for line in stream:
                    digest.update(line.encode())
                    row=json.loads(line)
                    if row.get('event')!=event:continue
                    timestamp=float(row[field])
                    if not math.isfinite(timestamp) or not start<=timestamp<end:continue
                    key=row['message_id']
                    earliest[key]=min(timestamp,earliest.get(key,math.inf))
            source_hashes['decoded_text:'+str(path.relative_to(ROOT))]=digest.hexdigest()
        times=[t for t in earliest.values() if start<=t<end]
        counts=np.histogram(times,bins=bins)[0]
        result[role]=(counts/np.diff(bins)).tolist()
    result['seconds']=((bins[:-1]+bins[1:])*.5-start).tolist()
    result['definition']='Distinct IDs at first observed completion/acknowledgment within evaluation, nonoverlapping 30-second bins during evaluation; warmup completions may contribute.'
    return result


def make(index, output):
    output.mkdir(parents=True,exist_ok=True)
    rows=json.loads(index.read_text()); hashes={}; reports=[]
    plt.rcParams.update({'font.size':9,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none'})
    def load(path):
        raw=path.read_bytes();hashes[str(path.relative_to(ROOT))]=hashlib.sha256(raw).hexdigest();return json.loads(raw)
    for row in rows:
        family,pair=row['family'],row['pair'];number=1 if pair.endswith('1') else 2
        fig,axes=plt.subplots(5,2,figsize=(12,16),layout='constrained');axes=axes.ravel()
        report=dict(family=family,run_number=number,comparison_id=pair,runs={})
        for action,record in row['runs'].items():
            directory=ROOT/'results'/record['run_id'];m=load(directory/'manifest.json')
            lag=load(directory/'lag-summary.json');prom=load(directory/'prometheus.json')
            start,end=m['evaluation_start_epoch'],m['producer_end_epoch'];duration=end-start
            try:
                rates=event_rates(directory,m,hashes)
            except OSError as exc:
                rates=dict(consumer=[],producer=[],seconds=[],archive_error=str(exc))

            outcomes=load(directory/'outcome-summary.json')
            expected=outcomes['useful_throughput_per_second']
            complete=bool(rates['consumer']) and math.isclose(sum(rates['consumer'])/len(rates['consumer']),expected,rel_tol=1e-9,abs_tol=1e-8)
            rates['consumer_timecourse_verified']=complete
            if complete:
                axes[0].plot(rates['seconds'],rates['consumer'],color=COLORS[action],label=LABELS[action]+' distinct completions')
            else:
                axes[0].axhline(expected,color=COLORS[action],ls='--',label=LABELS[action]+' saved run mean (curve unavailable)')
            # A partial producer archive must not be presented as achieved input.
            if complete and len(event_paths(directory/'producer'))==len(list((directory/'producer').rglob('final.json'))):
                axes[0].plot(rates['seconds'],rates['producer'],color=COLORS[action],ls=':',alpha=.7,label=LABELS[action]+' acknowledgments')
            snapshots=signals(lag['snapshots'])['snapshots']
            x=[s['timestamp']-start for s in snapshots]
            for ax,key in [(axes[1],'total_lag'),(axes[2],'processing_backlog'),(axes[3],'window_processing_backlog_growth_offsets_per_second'),(axes[8],'skew'),(axes[9],'max_partition_lag')]:
                xs=[];ys=[]
                for i,s in enumerate(snapshots):
                    if i and s.get('growth_offsets_per_second') is None:
                        xs.append(math.nan);ys.append(math.nan)
                    xs.append(x[i]);v=s.get(key);ys.append(v if s['valid'] and v is not None else math.nan)
                ax.plot(xs,ys,color=COLORS[action],label=LABELS[action],lw=1.5)
            availability={}
            for axis,role,suffix,factor in [(4,'consumer','cpu_percent',.01),(5,'consumer','memory_bytes',1/2**20),
                                           (6,'producer','cpu_percent',.01),(7,'producer','memory_bytes',1/2**20)]:
                selected=[s for s in prom['data']['result'] if s['metric'].get('__name__')==role+'_'+suffix and s['metric'].get('run_id')==m['run_id']]
                availability[role+'_'+suffix]=len(selected)
                for i,series in enumerate(sorted(selected,key=lambda s:(s['metric'].get('pod',''),s['metric'].get('incarnation','')))):
                    xs=[];ys=[];previous=None
                    for t,v in series['values']:
                        if not start<=t<=end:continue
                        if previous is not None and t-previous>3:xs.append(math.nan);ys.append(math.nan)
                        value=float(v);xs.append(t-start);ys.append(value*factor if math.isfinite(value) and value>=0 else math.nan);previous=t
                    axes[axis].plot(xs,ys,color=COLORS[action],alpha=.65,lw=.9,label=LABELS[action]+' (each process)' if i==0 else None)
                if not selected:axes[axis].text(.02,.9 if action=='none' else .8,LABELS[action]+': unavailable',transform=axes[axis].transAxes)
            report['runs'][action]=dict(run_id=m['run_id'],rates=rates,lag_coverage=lag['covered_fraction'],resource_series=availability,
                                       p99_seconds=record['p99'],unfinished_percent=record['unfinished_percent'])
        titles=[('Useful completion throughput and acknowledgments','Distinct messages/s'),('Consumer-position lag B','Offsets'),
                ('Processing backlog Q','Offsets'),('Processing-backlog growth, contiguous 30 s window','Offsets/s'),
                ('Consumer process CPU (individual traces)','CPU cores'),('Consumer process resident memory','MiB per process'),
                ('Producer process CPU (individual traces)','CPU cores'),('Producer process resident memory','MiB per process'),('Lag skew ratio: max(Bp) / mean(Bp)','Ratio'),('Maximum partition lag','Offsets')]
        anchor=m.get('intervention',{}).get('after_evaluation_start_seconds',60)
        for i,(ax,(title,ylabel)) in enumerate(zip(axes,titles)):
            ax.set(title=title,ylabel=ylabel,xlabel='Seconds from evaluation start',xlim=(0,duration))
            ax.axvline(anchor,color='#555555',ls='--',lw=.8)
            if i!=3:ax.set_ylim(bottom=0)
            else:ax.axhline(0,color='#777777',lw=.7)
            ax.grid(alpha=.18);ax.legend(fontsize=7,loc='best')
        cov='; '.join(LABELS[a]+': '+f"{100*v['lag_coverage']:.1f}%" for a,v in report['runs'].items())
        fig.suptitle(NAMES[family]+' | Run '+str(number)+' (two separate trials)',fontsize=14)
        fig.supxlabel('Dashed vertical line: scheduled scaling time in the scaling trial. Evaluation only; warm-up and drain omitted.\n'
                      +'Lag interval coverage: '+cov+'. Gaps are not zeros.\n'
                      +'CPU/RSS: historical process gauges, original resource scrape freshness unavailable; diagnostic only, not whole-container usage.',fontsize=8)
        stem=family+'-run'+str(number)
        for ext in ['png','svg']:fig.savefig(output/(stem+'.'+ext),dpi=160)
        plt.close(fig);reports.append(report)
        (output/(stem+'-data.json')).write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
        print('Saved',stem,flush=True)
    (output/'plot-data.json').write_text(json.dumps(reports,indent=2,allow_nan=False)+'\n')
    (output/'source-hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
    # Keep both repetitions visible for every workload rather than pool them.
    fig,axes=plt.subplots(6,2,figsize=(12,15),layout='constrained')
    for i,family in enumerate(NAMES):
        subset=[r for r in rows if r['family']==family]
        for j,(key,title) in enumerate([('p99','Completion p99 (s)'),('unfinished_percent','Unfinished at drain (%)')]):
            ax=axes[i,j]
            for action,shift in [('none',-.18),('scale',.18)]:
                vals=[r['runs'][action][key] for r in subset]
                bars=ax.bar(np.arange(2)+shift,vals,width=.34,color=COLORS[action],label=LABELS[action])
                ax.bar_label(bars,fmt='%.2f',fontsize=8,padding=3)
            ax.set_xticks([0,1],['Run 1','Run 2']);ax.set_title(NAMES[family]+'\n'+title,fontsize=10)
            ax.margins(y=.23);ax.set_ylim(bottom=0)
            if key=='unfinished_percent':ax.set_ylim(0,100)
            ax.legend(fontsize=7);ax.grid(axis='y',alpha=.15)
    fig.suptitle('All 24 completed trials | two trials per run number',fontsize=15)
    fig.supxlabel('p99 is conditional on evaluation-cohort completion by drain. Unfinished work is reported separately.\nEarlier starting conditions were recorded; matching was required only for 80/20 and the later single-partition comparisons.',fontsize=9)
    for ext in ['png','svg']:fig.savefig(output/('all-outcomes.'+ext),dpi=170)
    plt.close(fig)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--index',type=Path,default=ROOT/'experiment-records/paired-table-20260914/paired-results-data.json')
    a=p.parse_args();make(a.index,a.output)
