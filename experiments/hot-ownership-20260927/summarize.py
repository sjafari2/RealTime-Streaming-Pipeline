"""Save a reproducible comparison of two validated, fixed-ownership trials."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/hot-ownership-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from analyze_stability import window
from evidence_io import event_paths, open_events


def read(directory, name):
    return json.loads((directory / name).read_text())


def load_run(row):
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
    hot_counts = [4,4,4] if row['layout'] == 'distributed' else [12,0,0]
    for i in range(3):
        owned = {int(p) for p,owner in expected.items() if owner == f'consumer-sts-{i}'}
        assert len(owned) == 20 and len(owned & hot) == hot_counts[i]
    partitions = outcome['partition_metrics']['partitions']
    hot_count = sum(r['admitted_messages'] for r in partitions if r['partition'] in hot)
    start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
    observed_rates = {pod: sum(r['admitted_messages'] for r in partitions
                              if expected[str(r['partition'])] == pod)/(end-start)
                      for pod in identities}
    summary = dict(layout=row['layout'], run_id=manifest['run_id'],
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    campaign = read(args.campaign, 'campaign-status.json')
    assert campaign['status'] == 'complete' and campaign['restoration'] == 'verified'
    assert [r['layout'] for r in campaign['runs']] == ['distributed','concentrated']
    runs = [load_run(r) for r in campaign['runs']]
    args.output.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False})
    fig, axes = plt.subplots(1,2,figsize=(12,4.5),sharey=True)
    lag_top = max(150, math.ceil(max(run[3]['peak_lag'] for run in runs)*1.12/20)*20)
    diagnostic, panels = plt.subplots(5,2,figsize=(12,15),sharex='col',sharey='row')
    summaries = []
    for col, (p,m,lag,s) in enumerate(runs):
        summaries.append(s)
        title = 'Distributed: 4 / 4 / 4 hot partitions' if col == 0 else 'Concentrated: 12 / 0 / 0 hot partitions'
        x,y,owners,growth = lag_lines(m,lag)
        ax = axes[col]
        ax.plot(x,y,color='#176b93',linewidth=1.15)
        ax.set(title=title,xlabel='Evaluation time (minutes)',ylabel='Total lag (offsets)',xlim=(0,5),ylim=(0,lag_top))
        ax.grid(axis='y',alpha=.2)
        ax.text(.02,.96,f"Peak: {s['peak_lag']:,.0f}",transform=ax.transAxes,va='top',fontsize=10)
        series=read(p,'prometheus.json')['data']['result']
        for i in range(3):
            label=f'Consumer {i}'
            panels[0,col].plot(x,owners[i],label=label,linewidth=1)
            for row,name,divisor in [(2,'consumer_cpu_percent',100),(3,'consumer_memory_bytes',1024**2)]:
                rx,ry=resource_line(series,name,f'consumer-sts-{i}',m['evaluation_start_epoch'],m['producer_end_epoch'],divisor)
                panels[row,col].plot(rx,ry,label=label,linewidth=1)
        panels[1,col].plot(x,growth,color='#176b93',linewidth=1)
        panels[1,col].axhline(0,color='#777777',linewidth=.6)
        tx, ty = throughput_bins(p, m['evaluation_start_epoch'], m['producer_end_epoch'])
        panels[4,col].plot(tx,ty[0],label='Acknowledged input',color='#6b7280')
        panels[4,col].plot(tx,ty[1],label='Unique completions',color='#176b93')
        panels[4,col].legend(frameon=False,fontsize=8)
        panels[0,col].set_title(title)
        for row,label in enumerate(['Lag by owner (offsets)','Backlog growth (offsets/s)\n30-second window','Process CPU (cores)','Process memory (MiB)','Throughput (messages/s)\n10-second bins']):
            panels[row,col].set(ylabel=label,xlim=(0,5))
            panels[row,col].grid(axis='y',alpha=.2)
        panels[0,col].legend(frameon=False,fontsize=8)
        panels[4,col].set_xlabel('Evaluation time (minutes)')
    fig.suptitle('Same 80/20 input, different partition ownership — 700 messages/s',fontweight='bold')
    fig.text(.06,.02,'3 consumers • 60 partitions • 1 min warm-up + 5 min evaluation + 2 min drain.',fontsize=9,color='#555555')
    fig.tight_layout(rect=(0,.07,1,.93))
    diagnostic.suptitle('Ownership calibration — 700 messages/s, three consumers',fontweight='bold')
    diagnostic.text(.07,.015,'Evaluation interval only. Lines stop at unavailable samples. CPU and memory describe consumer processes, not entire nodes.',fontsize=9,color='#555555')
    diagnostic.tight_layout(rect=(0,.04,1,.96))
    for figure,name in [(fig,'ownership-lag'),(diagnostic,'ownership-diagnostics')]:
        for ext in ('png','pdf'): figure.savefig(args.output/(name+'.'+ext),dpi=180,facecolor='white')
    result=dict(execution_revision=campaign['code_commit'],design=campaign['design'],runs=summaries,
                limitations=['One trial per layout, fixed order, five-minute evaluation, shared machines.',
                             'Explicit assignment in both layouts; no live redistribution or scaling tested.',
                             'Completed-message latency must be read alongside unfinished work.',
                             'Monitoring gaps are retained, and finite observations cannot establish permanent stability.'])
    (args.output/'comparison.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    text=['# Hot-partition ownership: initial calibration','',
          'Both trials used 700 aggregate messages/s, three consumers, and the same 80/20 workload across 60 partitions. Each lasted eight minutes: one minute warm-up, five minutes evaluation and two minutes drain. Every consumer owned 20 partitions. Exact starting ownership was verified before releasing traffic.','',
          '| Starting layout | Mean lag (offsets) | Peak lag (offsets) | Completion p99 (s) | Unfinished at cutoff | Useful throughput (msg/s) |',
          '|---|---:|---:|---:|---:|---:|']
    for s in summaries:
        text.append(f"| {s['layout'].capitalize()} | {s['mean_lag']:.1f} | {s['peak_lag']:,.0f} | {s['p99_seconds']:.3f} | {s['unfinished']:,} / {s['admitted']:,} ({s['unfinished_percent']:.3f}%) | {s['useful_throughput']:.2f} |")
    text += ['', '| Layout | Consumer | Observed input (msg/s) | Mean process CPU (cores) | Mean process RSS (MiB) |',
             '|---|---|---:|---:|---:|']
    for s in summaries:
        for i in range(3):
            pod = f'consumer-sts-{i}'
            resources = {r['metric']: r['windows']['evaluation']['time_weighted_mean']
                         for r in s['process_resources'] if r['pod'] == pod}
            text.append(f"| {s['layout'].capitalize()} | {i} | {s['measured_input_by_owner'][pod]:.2f} | {resources['consumer_cpu_percent']/100:.3f} | {resources['consumer_memory_bytes']/1024**2:.1f} |")
    coverage = ', '.join(f"{s['layout']}: {100*s['lag_coverage']:.2f}%" for s in summaries)
    growth = ', '.join(f"{s['layout']}: {s['processing_backlog_windows']['whole_evaluation']['covered_interval_growth_offsets_per_second']:+.3f} offsets/s" for s in summaries)
    text += ['', 'Mean lag is time-weighted over valid observations. Lag coverage: ' + coverage + '.', '',
             'Whole-evaluation processing-backlog growth: ' + growth + '. These endpoint-based summaries are sensitive to short fluctuations; inspect the retained time series.', '',
             'Useful throughput counts distinct messages completed during evaluation, including any warm-up messages finishing in that interval. CPU and RSS means cover observed fresh intervals; the JSON records resource coverage separately.']
    text += ['', 'The distributed layout assigned four hot partitions to each consumer. The concentrated layout assigned all twelve to Consumer 0. Its expected input, including cold partitions, was 583.33 msg/s, versus 58.33 msg/s on each other consumer.', '',
             'These are initial-layout calibrations, once each. They do not measure the benefit or interruption cost of a live redistribution action, nor establish long-term stability. Consumer machine placement was recorded; machines were not newly pinned. Warm-up work remained in the pipeline. Latency excludes unfinished messages and ends before commit acknowledgment.', '',
             'The JSON summary retains exact outcomes, deadline results, actual traffic by initial owner, backlog growth windows, process CPU/RSS, resource requests, configuration, execution revision and evidence hashes. Raw events and Prometheus exports remain in the run evidence folders.', '',
             '![Lag comparison](ownership-lag.png)', '', '![Consumer diagnostics](ownership-diagnostics.png)', '']
    (args.output/'RESULTS.md').write_text('\n'.join(text))
    print(json.dumps([{k:s[k] for k in ('layout','run_id','admitted','unfinished','p99_seconds','mean_lag','peak_lag','lag_coverage')} for s in summaries],indent=2))


if __name__=='__main__': main()
