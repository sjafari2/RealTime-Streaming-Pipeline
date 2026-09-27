"""Compare validated fixed-ownership calibrations, retaining gaps and run provenance."""
import argparse
import hashlib
import json
import math
import os
import statistics
from pathlib import Path
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/hot-ownership-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from analyze_stability import window
from evidence_io import event_paths, open_events


LAYOUTS = {
    'distributed': ('Distributed hot partitions', [4,4,4]),
    'concentrated': ('All hot partitions on Consumer 0', [12,0,0]),
    'concentrated-c2': ('All hot partitions on Consumer 2', [0,0,12]),
    'concentrated-c1': ('All hot partitions on Consumer 1', [0,12,0]),
}


def read(directory, name):
    return json.loads((directory / name).read_text())


def load_run(row):
    if row['layout'] not in LAYOUTS:
        raise ValueError('Unknown ownership layout')
    p = Path(row['directory'])
    manifest = read(p, 'manifest.json')
    cfg = manifest['config']
    required = dict(TRAFFIC_MODE='skew', CONSUMER_ASSIGNMENT_MODE='explicit',
                    CONSUMER_STATIC_MEMBERSHIP='false', NUM_PARTITIONS='60', TOPIC_COUNT='1',
                    PRODUCER_POD_COUNT='3', CONSUMER_POD_COUNT='3', APP_CPU_ITERATIONS='2000',
                    APP_DELAY_MS='0', EXP_DURATION_SEC='360', WARMUP_SECONDS='60', DRAIN_SECONDS='120',
                    WORKLOAD_SEED='71', MITIGATION_POLICY='none')
    assert all(cfg.get(k) == v for k,v in required.items())
    assert float(cfg['TARGET_RATE'])*3 == 700 and float(cfg['SKEW_FRACTION']) == .8
    assert cfg['EXP_ID'] == 'hot-ownership-rate700-' + row['layout'] + '-run1'
    assert manifest['intervention']['action'] == 'none'
    lag = read(p, 'lag-summary.json')
    outcome = read(p, 'outcome-summary.json')
    audit = read(p, 'measurement-audit.json')
    assert read(p, 'runner-status.json')['status'] == 'complete'
    assert not outcome['validity_failures'] and not audit['issues']
    verify = manifest['explicit_start_verification']
    assert verify['verified']
    expected = {str(r['partition']): r['owner'] for r in verify['ownership']}
    identities = verify['incarnations']
    ownership_bad = []
    for s in lag['snapshots']:
        if not s['valid']:
            continue
        for key, value in s['owners'].items():
            partition = key.rsplit('/', 1)[1]
            pod = expected[partition]
            if value != pod + '/' + identities[pod]:
                ownership_bad.append(s['timestamp'])
                break
    assert not ownership_bad, 'Observed ownership changed in a fixed-layout trial'
    finals = [json.loads(x.read_text()) for x in p.glob('consumer/**/final.json')]
    assert sorted((x['pod'], x['incarnation']) for x in finals) == sorted(identities.items())
    hot = set(map(int, manifest['config']['SKEW_PARTITION'].split(',')))
    assert hot == set(range(12)) and set(expected) == {str(i) for i in range(60)}
    hot_counts = LAYOUTS[row['layout']][1]
    for i in range(3):
        owned = {int(p) for p,owner in expected.items() if owner == f'consumer-sts-{i}'}
        assert len(owned) == 20 and len(owned & hot) == hot_counts[i]
    partitions = outcome['partition_metrics']['partitions']
    hot_count = sum(r['admitted_messages'] for r in partitions if r['partition'] in hot)
    start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
    observed_rates = {pod: sum(r['admitted_messages'] for r in partitions
                              if expected[str(r['partition'])] == pod)/(end-start)
                      for pod in identities}
    skews = [s['skew'] for s in lag['snapshots'] if s['valid']]
    summary = dict(layout=row['layout'], label=LAYOUTS[row['layout']][0], run_id=manifest['run_id'],
        execution_revision=row.get('execution_revision'),
        raw_evidence='results/' + manifest['run_id'],
        lag_skew=dict(mean_valid_snapshot_ratio=statistics.mean(skews),
                      peak_snapshot_ratio=max(skews), minimum_snapshot_ratio=min(skews),
                      valid_snapshot_count=len(skews), window_samples=lag['parameters']['window_samples'],
                      definition='Maximum partition lag / mean partition lag; zero when all lag is zero. The snapshot mean is arithmetic, not the ratio of aggregated lags.'),
        admitted=outcome['admitted_evaluation_cohort'], completed=outcome['completed_by_drain'],
        unfinished=outcome['incomplete_by_drain'], unfinished_percent=100*outcome['incomplete_fraction'],
        p99_seconds=outcome['admitted_cohort_completion_p99_seconds'],
        mean_completion_seconds=outcome['admitted_cohort_completion_mean_seconds'],
        useful_throughput=outcome['useful_throughput_per_second'],
        admitted_rate=outcome['admitted_messages_per_second'],
        deadline_miss_fraction=outcome['deadline_miss_fraction'],
        deadline_seconds=float(manifest['config']['SLO_THRESHOLD_MS'])/1000,
        duplicate_completion_attempts=outcome['duplicate_completion_attempts'],
        partition_correctness=outcome['partition_metrics']['correctness'],
        mean_lag=lag['time_weighted_mean_lag'], peak_lag=lag['peak_sampled_lag'],
        lag_coverage=lag['covered_fraction'], actual_hot_fraction=hot_count/outcome['admitted_evaluation_cohort'],
        measured_input_by_owner=observed_rates,
        processing_backlog_windows={
            'whole_evaluation': window(lag['snapshots'], start, end),
            'last_half_evaluation': window(lag['snapshots'], (start+end)/2, end)},
        ownership_verified_at_start_and_valid_snapshots=True,
        initial_resources=manifest['initial_consumer_resources'],
        process_resources=audit['process_resources'],
        requested_resources=read(p,'execution-summary.json')['resource_requests'],
        config=manifest['config'],
        evidence_hashes={name: hashlib.sha256((p/name).read_bytes()).hexdigest() for name in
            ('manifest.json','runner-status.json','lag-summary.json','outcome-summary.json',
             'execution-summary.json','measurement-audit.json','prometheus.json')})
    return p, manifest, lag, summary


def lag_lines(manifest, lag):
    x, total, per_owner, growth = [], [], [[],[],[]], []
    previous = None
    for row in lag['snapshots']:
        t = (row['timestamp']-manifest['evaluation_start_epoch'])/60
        valid = row['valid'] and row.get('total_lag') is not None
        if valid and previous is not None:
            broken = (row['timestamp']-previous['timestamp'] > lag['parameters']['max_gap'] or
                      row['owners'] != previous['owners'] or any(row[field][k] < v
                      for field in ('highs','positions') for k,v in previous[field].items()))
            if broken:
                x.append(t); total.append(np.nan); growth.append(np.nan)
                for values in per_owner: values.append(np.nan)
        x.append(t); total.append(row['total_lag'] if valid else np.nan)
        value = row.get('window_processing_backlog_growth_offsets_per_second')
        growth.append(value if valid and value is not None else np.nan)
        for i, values in enumerate(per_owner):
            values.append(sum(v for k,v in row.get('lags',{}).items()
                              if row['owners'][k].split('/')[0] == f'consumer-sts-{i}') if valid else np.nan)
        previous = row if valid else None
    return x, total, per_owner, growth



def skew_lines(manifest, lag):
    """Show unavailable intervals as gaps, including missing snapshot timestamps."""
    x, raw, smooth = [], [], []
    previous = None
    for row in lag['snapshots']:
        t = (row['timestamp']-manifest['evaluation_start_epoch'])/60
        valid = row['valid'] and row.get('skew') is not None
        if valid and previous is not None:
            broken = (not 0 < row['timestamp']-previous['timestamp'] <= lag['parameters']['max_gap'] or
                      row['owners'] != previous['owners'] or any(row[field][k] < v
                      for field in ('highs','positions') for k,v in previous[field].items()))
            if broken:
                x.append(t); raw.append(np.nan); smooth.append(np.nan)
        x.append(t)
        raw.append(row['skew'] if valid else np.nan)
        value = row.get('window_mean_skew')
        smooth.append(value if valid and value is not None else np.nan)
        previous = row if valid else None
    return x, raw, smooth


def resource_line(series, name, pod, start, end, divisor):
    chosen = [s for s in series if s['metric'].get('__name__') == name and s['metric'].get('pod') == pod]
    stamps = [s for s in series if s['metric'].get('__name__') == name+'_scrape_timestamp_seconds' and s['metric'].get('pod') == pod]
    assert len(chosen) == len(stamps) == 1
    timestamps = {float(t):float(v) for t,v in stamps[0]['values']}
    x, y = [], []
    previous = None
    for t, value in chosen[0]['values']:
        t, value = float(t), float(value)
        if not start <= t <= end: continue
        if previous is not None and t-previous > 3:
            x.append((t-start)/60); y.append(np.nan)
        fresh = t in timestamps and 0 <= t-timestamps[t] <= 10
        x.append((t-start)/60); y.append(value/divisor if fresh and math.isfinite(value) and value >= 0 else np.nan)
        previous = t
    return x, y


def throughput_bins(directory, start, end, width=10):
    admitted, completed = {}, {}
    for path in event_paths(directory):
        with open_events(path) as stream:
            for line in stream:
                event = json.loads(line)
                if event['event'] == 'acknowledged':
                    target, timestamp = admitted, event['producer_timestamp']
                elif event['event'] == 'completed':
                    target, timestamp = completed, event['completion_timestamp']
                else:
                    continue
                if start <= timestamp < end:
                    identity = event['message_id']
                    target[identity] = min(target.get(identity, timestamp), timestamp)
    edges = np.arange(start, end + width, width)
    x = ((edges[:-1]+edges[1:])/2-start)/60
    return x, [np.histogram(list(values.values()), bins=edges)[0]/np.diff(edges)
               for values in (admitted, completed)]


DISPLAY_ORDER = ('distributed', 'concentrated', 'concentrated-c1', 'concentrated-c2')
CONSUMER_COLORS = ('#1f77b4', '#ff7f0e', '#2ca02c')


def diagnostic_data(runs):
    """Read each monitoring export and message-event stream once for all figures."""
    result = {}
    for p,m,lag,s in runs:
        x,_,owners,growth = lag_lines(m,lag)
        series = read(p,'prometheus.json')['data']['result']
        data = {'owners': [(x,y) for y in owners], 'growth': [(x,growth)]}
        for key,name,divisor in [('cpu','consumer_cpu_percent',100),
                                 ('memory','consumer_memory_bytes',1024**2)]:
            data[key] = [resource_line(series,name,f'consumer-sts-{i}',
                          m['evaluation_start_epoch'],m['producer_end_epoch'],divisor) for i in range(3)]
        tx,ty = throughput_bins(p,m['evaluation_start_epoch'],m['producer_end_epoch'])
        data['throughput'] = [(tx,y) for y in ty]
        result[s['layout']] = data
    return result


def metric_lines(ax, curves, metric, linewidth=1.3):
    if metric == 'throughput':
        labels,colors,styles = ('Acknowledged input','Unique completions'),('#64748b','#176b93'),('--','-')
    elif metric == 'growth':
        labels,colors,styles = ('Processing-backlog growth',),('#176b93',),('-',)
    else:
        labels,colors,styles = tuple(f'Consumer {i}' for i in range(3)),CONSUMER_COLORS,('-',)*3
    handles = []
    for (x,y),label,color,style in zip(curves,labels,colors,styles):
        handles.extend(ax.plot(x,y,label=label,color=color,linestyle=style,linewidth=linewidth))
    return handles


def finite_values(curves):
    return [v for _,ys in curves for v in ys if math.isfinite(v)]


def plot_metric_grid(runs, data, metric):
    """Compare one metric across layouts using a common scale and consumer colors."""
    titles = {'owners': 'Lag by consumer', 'growth': 'Processing-backlog growth',
              'cpu': 'Consumer CPU usage', 'memory': 'Consumer memory usage',
              'throughput': 'Input and completion throughput'}
    labels = {'owners': 'Lag by owner (offsets)', 'growth': 'Backlog growth (offsets/s)',
              'cpu': 'Process CPU (cores)', 'memory': 'Process RSS (MiB)',
              'throughput': 'Messages/s (10-second bins)'}
    values = {s['layout']: finite_values(data[s['layout']][metric]) for _,_,_,s in runs}
    all_values = [v for rows in values.values() for v in rows]
    minimum,maximum = min(all_values),max(all_values)
    units = {'owners': 20000, 'growth': 25, 'cpu': .1, 'memory': 25, 'throughput': 100}
    unit = units[metric]
    upper = max(unit,math.ceil(maximum*1.10/unit)*unit)
    lower = min(0,math.floor(minimum*1.10/10)*10) if metric == 'growth' else 0
    # A small-scale inset preserves fluctuations without changing the main comparison scale.
    small = []
    if metric in ('owners','growth') and maximum > (1000 if metric == 'owners' else 100):
        small = [layout for layout,ys in values.items() if max(abs(v) for v in ys) < maximum/10]
    small_values = [v for layout in small for v in values[layout]]
    detail_limits = None
    if small_values:
        if metric == 'owners':
            detail_limits = (0,max(20,math.ceil(max(small_values)*1.15/20)*20))
        else:
            extent = max(1,math.ceil(max(abs(v) for v in small_values)*1.15))
            detail_limits = (-extent,extent)
    columns = min(2,len(runs)); rows = math.ceil(len(runs)/columns)
    figure,axes = plt.subplots(rows,columns,figsize=(6*columns,4.2*rows),
                               sharex=True,sharey=True,squeeze=False)
    legend_handles = None
    for ax,(_,_,_,s) in zip(axes.flat,runs):
        curves = data[s['layout']][metric]
        handles = metric_lines(ax,curves,metric)
        if legend_handles is None: legend_handles = handles
        if metric == 'growth': ax.axhline(0,color='#888888',linewidth=.65,zorder=0)
        ax.set(title=s['label'],xlabel='Evaluation time (minutes)',ylabel=labels[metric],
               xlim=(0,5),ylim=(lower,upper))
        ax.set_title(s['label'],fontsize=11,pad=9)
        ax.tick_params(labelleft=True,labelbottom=True)
        if metric == 'owners': ax.yaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.grid(axis='y',alpha=.2)
        if s['layout'] in small:
            inset = ax.inset_axes([.52,.36,.43,.40])
            metric_lines(inset,curves,metric,linewidth=.85)
            if metric == 'growth': inset.axhline(0,color='#888888',linewidth=.5,zorder=0)
            inset.set(xlim=(0,5),ylim=detail_limits,xticks=(0,2.5,5))
            inset.set_title('Expanded vertical scale',fontsize=8,pad=3)
            inset.tick_params(labelsize=7,pad=1)
            inset.set_facecolor('#fafafa')
            inset.grid(axis='y',alpha=.15)
            for spine in inset.spines.values():
                spine.set_visible(True);spine.set_color('#aaaaaa');spine.set_linewidth(.6)
    for ax in list(axes.flat)[len(runs):]: ax.set_visible(False)
    figure.suptitle(titles[metric],fontsize=16,fontweight='bold',y=.985)
    subtitle = '700 messages/s total · Three consumers · 80% of input across 12 of 60 partitions'
    figure.text(.5,.947,subtitle,ha='center',fontsize=10,color='#555555')
    if metric != 'growth':
        figure.legend(handles=legend_handles,loc='upper center',bbox_to_anchor=(.5,.929),
                      ncol=len(legend_handles),frameon=False,fontsize=10)
    note = 'Five-minute evaluation. Main panels share the same vertical scale.'
    if detail_limits is not None: note += ' Insets show the same data with a shared expanded scale.'
    if metric == 'growth': note += '\nGrowth uses a rolling 30-second window; gaps remain unavailable.'
    elif metric == 'cpu': note += '\nProcess measurements; 1 CPU core corresponds to 100% CPU use.'
    elif metric == 'memory': note += '\nMemory is process resident set size (RSS).'
    elif metric == 'throughput': note += '\nUnique completions can include warm-up records finishing during evaluation.'
    figure.text(.06,.025,note,fontsize=9,color='#555555')
    figure.tight_layout(rect=(0,.09,1,.895))
    return figure



def plot_diagnostics(runs, data):
    """Keep at most two layouts in each diagnostic figure so labels stay legible."""
    figure, panels = plt.subplots(5,len(runs),figsize=(6*len(runs),15),
                                 sharex='col',sharey='row',squeeze=False)
    for col, (p,m,lag,s) in enumerate(runs):
        x,_,owners,growth = lag_lines(m,lag)
        for i in range(3):
            label = f'Consumer {i}'
            panels[0,col].plot(x,owners[i],label=label,linewidth=1)
            for row,name,divisor in [(2,'consumer_cpu_percent',100),(3,'consumer_memory_bytes',1024**2)]:
                rx,ry = data[s['layout']]['cpu' if row == 2 else 'memory'][i]
                panels[row,col].plot(rx,ry,label=label,linewidth=1)
        panels[1,col].plot(x,growth,color='#176b93',linewidth=1)
        panels[1,col].axhline(0,color='#777777',linewidth=.6)
        tx,ty0 = data[s['layout']]['throughput'][0]
        _,ty1 = data[s['layout']]['throughput'][1]
        panels[4,col].plot(tx,ty0,label='Acknowledged input',color='#6b7280')
        panels[4,col].plot(tx,ty1,label='Unique completions',color='#176b93')
        panels[4,col].legend(frameon=False,fontsize=8)
        panels[0,col].set_title(s['label'],fontsize=11)
        for row,label in enumerate(['Lag by owner (offsets)','Backlog growth (offsets/s)\n30-second window',
                                    'Process CPU (cores)','Process memory (MiB)',
                                    'Throughput (messages/s)\n10-second bins']):
            panels[row,col].set(ylabel=label,xlim=(0,5))
            panels[row,col].grid(axis='y',alpha=.2)
        panels[0,col].legend(frameon=False,fontsize=8)
        panels[4,col].set_xlabel('Evaluation time (minutes)')
    figure.suptitle('Ownership calibration — 700 messages/s, three consumers',fontweight='bold')
    figure.text(.07,.015,'Evaluation only. Gaps remain unavailable. CPU and memory describe consumer processes.',fontsize=9,color='#555555')
    figure.tight_layout(rect=(0,.04,1,.96))
    return figure


def load_campaigns(paths):
    campaigns, rows, seen = [], [], set()
    for path in paths:
        campaign = read(path,'campaign-status.json')
        if campaign['status'] != 'complete' or campaign.get('restoration') != 'verified':
            raise ValueError('Campaign must complete and restore its settings before comparison')
        for row in campaign['runs']:
            if row['layout'] in seen:
                raise ValueError('This comparison expects one trial per layout, not pooled repeats')
            seen.add(row['layout'])
            rows.append(dict(row,execution_revision=campaign['code_commit']))
        campaigns.append(dict(name=path.name,execution_revision=campaign['code_commit'],
                              design=campaign['design'],started_epoch=campaign['started_epoch'],
                              finished_epoch=campaign['finished_epoch']))
    if not 1 <= len(rows) <= 4:
        raise ValueError('Select one to four distinct reviewed layouts')
    return campaigns, rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign',type=Path,nargs='+',help='Completed campaign directories, in execution order')
    parser.add_argument('output',type=Path)
    parser.add_argument('--individual-lag', choices=tuple(LAYOUTS), nargs='*', default=[],
                        help='Also save a full-size lag figure for selected layouts')
    args = parser.parse_args()
    campaigns,rows = load_campaigns(args.campaign)
    runs = [load_run(row) for row in rows]
    display_runs = sorted(runs,key=lambda run: DISPLAY_ORDER.index(run[3]['layout']))
    data = diagnostic_data(runs)
    if len({r[1]['config']['TOPIC_TITLE'] for r in runs}) != len(runs):
        raise ValueError('Separate ownership trials must use distinct run-specific topics')
    if not set(args.individual_lag) <= {r[3]['layout'] for r in runs}:
        parser.error('Individual lag plots must refer to layouts in the selected campaigns')
    args.output.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False})
    columns = min(2,len(runs)); lines = math.ceil(len(runs)/columns)
    figure,axes = plt.subplots(lines,columns,figsize=(6*columns,4.2*lines),squeeze=False)
    skew_figure,skew_axes = plt.subplots(lines,columns,figsize=(6*columns,4.2*lines),sharey=True,squeeze=False)
    # A common low-range axis preserves detail. Much larger ranges are labeled separately.
    common_top = max(160,math.ceil(max(run[3]['peak_lag'] for run in runs)*1.12/20)*20)
    separate_scales = common_top > 1000 and min(run[3]['peak_lag'] for run in runs)*10 < common_top
    skew_top = max(12,math.ceil(max(run[3]['lag_skew']['peak_snapshot_ratio'] for run in runs)*1.1))
    summaries = [run[3] for run in runs]  # Preserve execution order in the evidence record.
    for index,(p,m,lag,s) in enumerate(display_runs):
        x,y,_,_ = lag_lines(m,lag)
        ax = axes.flat[index]
        top = max(160,math.ceil(s['peak_lag']*1.12/20)*20) if separate_scales else common_top
        ax.plot(x,y,color='#176b93',linewidth=1.15)
        ax.set(title=s['label'],xlabel='Evaluation time (minutes)',ylabel='Total lag (offsets)',xlim=(0,5),ylim=(0,top))
        ax.grid(axis='y',alpha=.2)
        ax.text(.02,.96,f"Peak: {s['peak_lag']:,.0f}",transform=ax.transAxes,va='top',fontsize=10)
        sx,raw,smooth = skew_lines(m,lag)
        ax = skew_axes.flat[index]
        ax.plot(sx,raw,color='#94a3b8',alpha=.6,linewidth=.7,label='Snapshot ratio')
        ax.plot(sx,smooth,color='#176b93',linewidth=1.4,label='15-sample rolling mean')
        ax.set(title=s['label'],xlabel='Evaluation time (minutes)',ylabel='Partition lag skew ratio',xlim=(0,5),ylim=(0,skew_top))
        ax.grid(axis='y',alpha=.2)
        if index == 0: ax.legend(frameon=False,fontsize=8)
    for index in range(len(runs),lines*columns):
        axes.flat[index].set_visible(False);skew_axes.flat[index].set_visible(False)
    figure.suptitle('Same 80/20 input, different ownership — 700 messages/s',fontweight='bold')
    note = 'Panels use different vertical scales.' if separate_scales else 'Panels share a vertical scale.'
    figure.text(.06,.02,'1 min warm-up + 5 min evaluation + 2 min drain. '+note,fontsize=9,color='#555555')
    figure.tight_layout(rect=(0,.06,1,.94))
    skew_figure.suptitle('Partition lag skew — 700 messages/s, three consumers',fontweight='bold')
    skew_figure.text(.06,.02,'Ratio = maximum partition lag / mean partition lag; zero if all lag is zero. Rolling mean: 15 consecutive valid samples.',fontsize=9,color='#555555')
    skew_figure.tight_layout(rect=(0,.06,1,.94))
    figures = [(figure,'ownership-lag'),(skew_figure,'ownership-skew')]
    for first in range(0,len(runs),2):
        name = 'ownership-diagnostics' + (f'-{first//2+1}' if first else '')
        figures.append((plot_diagnostics(runs[first:first+2],data),name))
    grid_figures = [(plot_metric_grid(display_runs,data,key),'ownership-'+key)
                    for key in ('cpu','memory','throughput','growth','owners')]
    figures.extend(grid_figures)
    for _,m,lag,s in runs:
        if s['layout'] not in args.individual_lag:
            continue
        single,ax = plt.subplots(figsize=(9,4.8))
        x,y,_,_ = lag_lines(m,lag)
        ax.plot(x,y,color='#176b93',linewidth=1.7)
        top = max(160,math.ceil(s['peak_lag']*1.12/20)*20)
        ax.set(title=s['label']+' — 700 messages/s total',xlabel='Evaluation time (minutes)',
               ylabel='Total lag (offsets)',xlim=(0,5),ylim=(0,top))
        ax.yaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.grid(axis='y',alpha=.2)
        ax.text(.02,.95,f"Peak lag: {s['peak_lag']:,.0f}",transform=ax.transAxes,va='top')
        single.text(.09,.025,'1 min warm-up + 5 min evaluation + 2 min drain. Plot shows evaluation only.',fontsize=10,color='#555555')
        single.tight_layout(rect=(0,.07,1,1))
        figures.append((single,s['layout']+'-lag'))
    for fig,name in figures:
        for suffix in ('png','pdf'):fig.savefig(args.output/(name+'.'+suffix),dpi=180,facecolor='white')
    with PdfPages(args.output/'ownership-metrics.pdf') as bundle:
        for fig,_ in grid_figures+[figures[0],figures[1]]:
            bundle.savefig(fig,facecolor='white')
    result = dict(campaigns=campaigns,runs=summaries,
                  limitations=['One trial per layout, fixed order, five-minute evaluation and shared machines.',
                               'Later layouts were selected during exploratory calibration, not a prespecified confirmatory comparison.',
                               'Explicit fixed assignment in every layout; no live redistribution or scaling tested.',
                               'Completed-message latency must be reported with unfinished work.',
                               'Finite observations do not establish permanent stability.'])
    (args.output/'comparison.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    text = ['# Hot-partition ownership calibration','',
            f'These {len(runs)} trials used 700 aggregate messages/s, three consumers and the same 80/20 workload across 60 partitions. Each lasted eight minutes: one warm-up, five evaluation and two drain. Every consumer owned 20 partitions. Exact starting ownership was verified before traffic.','',
            '| Layout | Mean lag (offsets) | Peak lag (offsets) | Completion p99 (s) | Unfinished at cutoff | Useful throughput (msg/s) |',
            '|---|---:|---:|---:|---:|---:|']
    for s in summaries:
        text.append(f"| {s['label']} | {s['mean_lag']:.1f} | {s['peak_lag']:,.0f} | {s['p99_seconds']:.3f} | {s['unfinished']:,} / {s['admitted']:,} ({s['unfinished_percent']:.3f}%) | {s['useful_throughput']:.2f} |")
    text += ['', '| Layout | Mean snapshot lag-skew ratio | Whole-evaluation backlog growth (offsets/s) | Lag coverage |',
             '|---|---:|---:|---:|']
    for s in summaries:
        growth = s['processing_backlog_windows']['whole_evaluation']['covered_interval_growth_offsets_per_second']
        text.append(f"| {s['label']} | {s['lag_skew']['mean_valid_snapshot_ratio']:.3f} | {growth:+.3f} | {100*s['lag_coverage']:.2f}% |")
    text += ['', '| Layout | Consumer | Observed input (msg/s) | Mean process CPU (cores) | Mean process RSS (MiB) |',
             '|---|---|---:|---:|---:|']
    for s in summaries:
        for i in range(3):
            pod = f'consumer-sts-{i}'
            resources = {r['metric']:r['windows']['evaluation']['time_weighted_mean'] for r in s['process_resources'] if r['pod']==pod}
            text.append(f"| {s['label']} | {i} | {s['measured_input_by_owner'][pod]:.2f} | {resources['consumer_cpu_percent']/100:.3f} | {resources['consumer_memory_bytes']/1024**2:.1f} |")
    text += ['', 'Mean lag is time-weighted over valid observations. Mean snapshot skew is the arithmetic mean of valid instantaneous maximum/mean lag ratios. Skew describes relative partition imbalance and must be interpreted with backlog magnitude and growth. A large ratio can occur with little absolute lag. Backlog-growth summaries use covered intervals without bridging gaps. The figures also show the rolling 30-second growth trace.', '',
             'Useful throughput counts distinct completions during evaluation, including any warm-up records finishing then. CPU and RSS means cover observed fresh intervals; resource coverage is retained separately in JSON. Cohort latency excludes unfinished records, retains the effect of queued warm-up work, and ends before commit acknowledgment.', '',
             'These are initial-layout calibrations, once each, with no live redistribution or scaling. No additional node-pinning constraint was introduced. Later concentration targets were selected after the earlier observations; these are exploratory calibration outcomes. Retain unfavorable valid results and do not infer a general mitigation benefit or permanent stability.', '',
             'The JSON retains configuration, exact outcomes, deadlines, skew, growth windows, resource data, per-run execution revisions and evidence hashes. Raw message evidence and full monitoring exports are stored separately; this summary is not a raw-data backup.', '']
    text += ['Figures use the same display order: distributed, Consumer 0, Consumer 1, Consumer 2. Tables and evidence retain actual execution order. Each metric grid uses shared main-panel scales; labeled insets expand small lag and growth fluctuations. The total-lag overview retains its explicitly labeled separate scales.', '', '[All comparison figures in one PDF](ownership-metrics.pdf)', '']
    for _,name in grid_figures+[figures[0],figures[1]]:
        text += [f'![{name}]({name}.png)','']
    (args.output/'RESULTS.md').write_text('\n'.join(text))
    print(json.dumps([{k:s[k] for k in ('layout','run_id','admitted','unfinished','p99_seconds','mean_lag','peak_lag','lag_coverage','lag_skew')} for s in summaries],indent=2))


if __name__ == '__main__': main()
