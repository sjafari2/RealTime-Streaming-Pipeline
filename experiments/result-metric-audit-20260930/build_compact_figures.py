"""Compact comparison panels and the proposal's windowed runtime state.

Reconstructs observations, not a controller that ran during the experiments.
Message identities provide outstanding work; missing monitoring stays missing.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import sys

from build_report import COLORS, LABELS, continuous
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
import numpy as np


def read(path):
    return json.loads(path.read_text())


def runtime_states(snapshots, start, finish):
    """Use original 15-interval growth, not the shorter five-interval plot."""
    output = []
    previous = None
    for r in snapshots:
        if not start <= r['timestamp'] <= finish:
            continue
        valid = bool(r.get('valid'))
        state = dict(timestamp=r['timestamp'], seconds=r['timestamp']-start,
            valid=valid, break_before=previous is not None and not continuous(previous, r),
            B_W=r.get('window_mean_backlog') if valid else None,
            G_W=r.get('window_growth_offsets_per_second') if valid else None,
            S_W=r.get('window_mean_skew') if valid else None,
            H_persistent=r.get('persistent_hot') if valid else None,
            raw_skew=r.get('skew') if valid else None,
            max_partition_lag=r.get('max_partition_lag') if valid else None,
            mean_partition_lag=r.get('mean_partition_lag') if valid else None)
        state['state_available'] = all(state[k] is not None for k in ('B_W','G_W','S_W','H_persistent'))
        output.append(state)
        previous = r
    return output


def lines(states, key):
    x, y = [], []
    for r in states:
        if r['break_before']:
            x.append(r['seconds']/60); y.append(np.nan)
        value = r[key]
        if key == 'H_persistent' and value is not None:
            value = len(value)
        x.append(r['seconds']/60); y.append(value if value is not None else np.nan)
    return x, y


def event_series(directory, start, finish):
    from evidence_io import event_paths, open_events
    ack, produced, done, starts, hashes = {}, {}, {}, {}, {}
    for path in event_paths(directory):
        h = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''): h.update(chunk)
        hashes[str(path.relative_to(directory))] = h.hexdigest()
        meta = read(path.parent/'final.json')
        with open_events(path) as stream:
            for line in stream:
                e = json.loads(line)
                if e['event']=='started' and meta['role']=='consumer':
                    starts[meta['pod']] = min(starts.get(meta['pod'],math.inf),e['timestamp'])
                elif e['event']=='acknowledged':
                    assert e['message_id'] not in ack
                    ack[e['message_id']] = e['timestamp']
                    produced[e['message_id']] = e['producer_timestamp']
                elif e['event']=='completed':
                    assert e['message_id'] not in done
                    done[e['message_id']] = e['completion_timestamp']
    assert set(done) <= set(ack)
    times = np.arange(start,finish+.01,2)
    # Count identities that are acknowledged and not yet completed at time t.
    arrivals = np.sort(list(ack.values()))
    eligible_done = np.sort([max(ack[k],v) for k,v in done.items()])
    outstanding = np.searchsorted(arrivals,times,side='right')-np.searchsorted(eligible_done,times,side='right')
    edges = np.arange(start,finish+10,10)
    a = [v for v in produced.values() if start <= v < finish]
    d = [v for v in done.values() if start <= v < finish]
    return dict(event_sha256=hashes,consumer_start_epoch=starts,
        admitted_evaluation=len(a),eval_completions=len(d),
        outstanding_minutes=((times-start)/60).tolist(),outstanding=outstanding.tolist(),
        throughput_minutes=((edges[:-1]+edges[1:])/2-start).tolist(),
        input_rate=(np.histogram(a,bins=edges)[0]/10).tolist(),
        completion_rate=(np.histogram(d,bins=edges)[0]/10).tolist())


def process_total(prom, name, divisor, starts, start, finish):
    """Sum only when every started process has a fresh observed value."""
    values, stamps, all_times = {}, {}, set()
    for s in prom:
        metric=s['metric']; pod=metric.get('pod')
        if pod not in starts: continue
        if metric.get('__name__') not in (name,name+'_scrape_timestamp_seconds'): continue
        target=values if metric['__name__']==name else stamps
        assert pod not in target, 'Ambiguous process series'
        target[pod]={float(t):float(v) for t,v in s['values'] if start<=float(t)<=finish}
        all_times.update(target[pod])
    x,y=[],[];previous=None
    for t in sorted(all_times):
        if previous is not None and t-previous>3:
            x.append((t-start)/60);y.append(None)
        active=[p for p,v in starts.items() if v<=t]
        row=[]
        for p in active:
            value=values.get(p,{}).get(t); stamp=stamps.get(p,{}).get(t)
            if value is None or stamp is None or not (math.isfinite(value) and value>=0 and 0<=t-stamp<=10):
                row=None;break
            row.append(value/divisor)
        x.append((t-start)/60);y.append(sum(row) if row and len(row)==len(active) else None)
        previous=t
    return dict(minutes=x,values=y)


def setup(rows, nrows, title, height, sharey='row'):
    fig,axes=plt.subplots(nrows,2,figsize=(8.1,height),sharex=False,sharey=sharey,squeeze=False)
    for col in range(2):
        axes[0,col].set_title(f'Run {col+1}',fontsize=11)
    arms=list(dict.fromkeys(r['arm'] for r in rows))
    handles=[Line2D([0],[0],color=COLORS[a],lw=2,label=LABELS[a]) for a in arms]
    fig.suptitle(title,fontsize=12,y=.993)
    fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.54,.966),ncol=2,fontsize=8,frameon=False)
    for ax in axes.flat:
        ax.grid(alpha=.2);ax.set_axisbelow(True)
    fig.subplots_adjust(left=.14,right=.99,bottom=.13,top=.87,wspace=.12,hspace=.24)
    return fig,axes


def time_axis(ax,duration):
    ax.set_xlim(0,duration/60);ax.axvline(1,color='#555555',ls=':',lw=.9)


def save(fig,destination,note):
    fig.text(.54,.014,note,ha='center',fontsize=8)
    for ext in ('pdf','png'): fig.savefig(destination.with_suffix('.'+ext),dpi=200)
    plt.close(fig)


def runtime_figure(rows,states,title,destination):
    fig,axes=setup(rows,5,title,9.1)
    keys=['B_W','G_W','S_W','H_persistent','max_partition_lag']
    labels=[r'$\overline{B}_W$ (offsets)',r'$\overline{G}_W$ (offsets/s)',r'$\overline{S}_W$ (ratio)',r'$|H_{\mathrm{persistent}}|$', 'Partition lag\n(offsets)']
    for r in rows:
        col=r['run_number']-1;series=states[r['run_id']]['states']
        for i,k in enumerate(keys):
            axes[i,col].plot(*lines(series,k),color=COLORS[r['arm']],lw=1,
                drawstyle='steps-post' if k=='H_persistent' else 'default')
        axes[4,col].plot(*lines(series,'mean_partition_lag'),color=COLORS[r['arm']],lw=.85,ls='--')
    for i,label in enumerate(labels):
        axes[i,0].set_ylabel(label)
        for col in range(2):
            ax=axes[i,col];time_axis(ax,rows[0]['evaluation_seconds'])
            if i!=1:ax.set_ylim(bottom=0)
            else:ax.axhline(0,color='#777777',lw=.5)
            if i in (0,4):ax.set_yscale('symlog',linthresh=1)
            if i==3:ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            if i==4:ax.set_xlabel('Minutes since evaluation began')
            else:ax.tick_params(labelbottom=False)
    save(fig,destination,'Runtime state: first four rows; final row: maximum (solid) and mean (dashed) lag.\n'
         'B, S and persistence: 15 samples; G: 15 intervals. Dotted: scheduled action. Gaps remain missing.\n'
         'B and partition-lag axes: linear near zero, logarithmic above one offset. Hot-set IDs are retained.')


def performance_figure(rows,details,title,destination):
    fig,axes=setup(rows,4,title,8.0)
    for r in rows:
        col=r['run_number']-1;d=details[r['run_id']];color=COLORS[r['arm']]
        for i,k in enumerate(('cpu','memory')):
            axes[i,col].plot(d[k]['minutes'],[np.nan if x is None else x for x in d[k]['values']],color=color,lw=1)
        axes[2,col].plot(np.array(d['throughput_minutes'])/60,d['completion_rate'],color=color,lw=1)
        x=[s['time_minutes'] for s in r['timeseries']]
        y=[np.nan if s['window_processing_backlog_growth_offsets_per_second'] is None else s['window_processing_backlog_growth_offsets_per_second'] for s in r['timeseries']]
        axes[3,col].plot(x,y,color=color,lw=1)
    for i,label in enumerate(['Actual CPU\n(cores)','Actual RSS\n(MiB)','Useful completions\n(msg/s)','Processing-backlog\ngrowth (offsets/s)']):
        axes[i,0].set_ylabel(label)
        for col in range(2):
            ax=axes[i,col];time_axis(ax,rows[0]['evaluation_seconds'])
            if i<3:ax.set_ylim(bottom=0)
            if i==2:ax.axhline(700,color='#777777',ls='--',lw=.8)
            if i==3:ax.axhline(0,color='#777777',lw=.5);ax.set_xlabel('Minutes since evaluation began')
            else:ax.tick_params(labelbottom=False)
    save(fig,destination,'CPU and RSS sum freshly observed active consumer processes. Unknown observations remain gaps.\n'
         'Throughput: distinct completions in 10-s bins; dashed: 700-msg/s input. Growth: 10-s window.\n'
         'Evaluation only; warm-up records completed here count toward throughput. Dotted: scheduled action.')


def outcome_figure(rows,details,title,destination):
    fig,axes=setup(rows,3,title,7.1,sharey=False)
    for r in rows:
        col=r['run_number']-1;color=COLORS[r['arm']];d=details[r['run_id']]
        axes[0,col].plot([s['time_minutes'] for s in r['timeseries']],
            [np.nan if s['total_lag'] is None else s['total_lag'] for s in r['timeseries']],color=color,lw=1.1)
        axes[1,col].plot(d['outstanding_minutes'],d['outstanding'],color=color,lw=1.1)
    for i,label in enumerate(['Total position lag\n(offsets)','Acknowledged but\nunfinished (messages)']):
        axes[i,0].set_ylabel(label)
        ymax=max(ax.get_ylim()[1] for ax in axes[i])
        for col in range(2):
            time_axis(axes[i,col],rows[0]['evaluation_seconds']);axes[i,col].set_ylim(0,ymax)
            axes[i,col].tick_params(labelsize=8)
            if i==0:axes[i,col].tick_params(labelbottom=False)
            else:axes[i,col].set_xlabel('Minutes since evaluation began',fontsize=8)
    arms=list(dict.fromkeys(r['arm'] for r in rows));width=.35
    for col,(key,label) in enumerate([('p99_seconds','Completion p99 (s)'),('unfinished_percent','Unfinished at cutoff (%)')]):
        ax=axes[2,col]
        for run in (1,2):
            values=[next(r[key] for r in rows if r['arm']==a and r['run_number']==run) for a in arms]
            pos=np.arange(len(arms))+(run-1.5)*width
            ax.bar(pos,values,width,color=[COLORS[a] for a in arms],hatch='//' if run==2 else None,edgecolor='white',linewidth=.6)
            for xx,value in zip(pos,values):
                if value==0:ax.text(xx,.02,'0',transform=ax.get_xaxis_transform(),ha='center',fontsize=8)
        short={'keep3':'Keep 3','redistribute3':'Redis. 3','scale_redistribute6':'Targeted 6','kafka_scale6':'Kafka 6'}
        ax.set_xticks(range(len(arms)),[short[a] for a in arms],fontsize=8,rotation=15)
        ax.set_ylabel(label,fontsize=9);ax.set_ylim(0,max(1,max(r[key] for r in rows)*1.1))
    fig.subplots_adjust(hspace=.5,wspace=.32,bottom=.16)
    save(fig,destination,'Top: evaluation only; outstanding count includes warm-up messages. Dotted: scheduled action.\n'
         'Bottom: evaluation-born cohort through drain cutoff; left bar: Run 1, hatched right bar: Run 2.\n'
         'P99 conditions on completion; no latency is assigned to unfinished records.')


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    args=ap.parse_args();root=args.root;out=args.output
    sys.path.insert(0,str(root/'python-scripts'))
    groups=read(out/'metrics.json')['groups'];states={};details={}
    cache=out/'plot-cache';cache.mkdir(exist_ok=True)
    plt.rcParams.update({'font.size':9,'axes.labelsize':9,'xtick.labelsize':8,'ytick.labelsize':8,'pdf.fonttype':42})
    for block,rows in groups.items():
        for r in rows:
            path=root/'results'/r['run_id'];m=read(path/'manifest.json');lag=read(path/'lag-summary.json')
            start,finish=m['evaluation_start_epoch'],m['producer_end_epoch']
            assert lag['parameters']['window_samples']==15
            assert lag['parameters'].get('growth_window_samples',15)==15
            states[r['run_id']]=dict(block=block,arm=r['arm'],run_number=r['run_number'],
                parameters=lag['parameters'],states=runtime_states(lag['snapshots'],start,finish))
            if block not in ('twelve-partition','four-partition'): continue
            print('Compact panels:',r['run_id'],flush=True)
            cached=cache/(r['run_id']+'.json')
            if cached.exists(): d=read(cached)
            else:
                d=event_series(path,start,finish);cached.write_text(json.dumps(d))
            assert d['event_sha256']==r['intervention_cost']['event_file_sha256'], 'Event evidence changed since the published comparison'
            assert d['admitted_evaluation']==r['admitted']
            assert math.isclose(d['eval_completions']/(finish-start),r['useful_throughput'])
            prom=read(path/'prometheus.json')['data']['result']
            for k,name,divisor in [('cpu','consumer_cpu_percent',100),('memory','consumer_memory_bytes',2**20)]:
                d[k]=process_total(prom,name,divisor,d['consumer_start_epoch'],start,finish)
            details[r['run_id']]=d
        title=('12' if block=='twelve-partition' else '4')+' high-input partitions at 700 messages/s'
        if block in ('twelve-partition','four-partition'):
            runtime_figure(rows,states,title+' — runtime state',out/'figures'/(block+'-runtime-state'))
            performance_figure(rows,details,title+' — performance and resources',out/'figures'/(block+'-compact-performance'))
            outcome_figure(rows,details,title+' — outcomes',out/'figures'/(block+'-compact-outcomes'))
    (out/'runtime-states.json').write_text(json.dumps(states,indent=2))
    # CSV preserves the set of partition IDs, not just the plotted count.
    with (out/'runtime-states.csv').open('w',newline='') as stream:
        names=['run_id','block','arm','run_number','seconds','B_W','G_W','S_W','H_persistent','state_available','break_before']
        writer=csv.DictWriter(stream,fieldnames=names,lineterminator='\n');writer.writeheader()
        for run,d in states.items():
            for s in d['states']:
                row={k:d[k] for k in ('block','arm','run_number')};row.update({k:s[k] for k in names if k in s})
                row.update(run_id=run,H_persistent=json.dumps(s['H_persistent']))
                writer.writerow(row)
    (out/'compact-plot-data.json').write_text(json.dumps(details,indent=2))
    print(f'Saved runtime states for {len(states)} trials and compact figures',flush=True)


if __name__=='__main__':main()
