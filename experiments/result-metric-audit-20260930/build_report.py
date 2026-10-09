"""Report matched intervention trials without changing their original evidence.

Monitoring covers evaluation. Cohort outcomes follow evaluation-born records to
the original drain cutoff. Resource integrals cover evaluation plus drain.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/results-metric-audit-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
import numpy as np

BLOCKS = {
    'twelve-partition': ('four-condition-20260929', 'Twelve high-input partitions; 10-minute evaluation'),
    'four-partition': ('four-hot-partitions-20260930', 'Four high-input partitions; 10-minute evaluation'),
}
LABELS = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute within 3',
          'scale_redistribute6': 'Scale + targeted redistribution',
          'scale6': 'Scale + targeted redistribution', 'kafka_scale6': 'Scale + Kafka rebalance'}
COLORS = {'keep3': '#2765a6', 'redistribute3': '#d97b1d',
          'scale_redistribute6': '#168361', 'scale6': '#168361', 'kafka_scale6': '#9653a6'}
FIELDS = ['total_lag', 'processing_backlog', 'mean_partition_lag', 'max_partition_lag',
          'population_stddev', 'skew', 'window_mean_backlog', 'window_mean_skew',
          'growth_offsets_per_second', 'window_growth_offsets_per_second',
          'processing_backlog_growth_offsets_per_second',
          'window_processing_backlog_growth_offsets_per_second', 'persistent_hot_count', 'hot_count']


def read(p):
    return json.loads(p.read_text())


def sha(p):
    h = hashlib.sha256()
    with p.open('rb') as stream:
        for b in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def continuous(a, b):
    if not (a.get('valid') and b.get('valid') and
            0 < b['timestamp'] - a['timestamp'] <= 3 and a['owners'] == b['owners']):
        return False
    return not any(b[field][p] < a[field][p] for field in ('positions', 'highs') for p in a[field])


def stats(points, field, duration):
    area = covered = 0.
    for a, b in zip(points, points[1:]):
        if not continuous(a, b) or a.get(field) is None or b.get(field) is None:
            continue
        dt = b['timestamp'] - a['timestamp']
        area += (a[field] + b[field]) * .5 * dt
        covered += dt
    values = [r[field] for r in points if r.get('valid') and r.get(field) is not None]
    return {'time_weighted_mean': area / covered if covered else None,
            'sampled_peak': max(values, default=None), 'covered_seconds': covered,
            'coverage_fraction': covered / duration, 'defined_samples': len(values)}


def compact_series(points, start):
    result = []
    for i, row in enumerate(points):
        if i and not continuous(points[i - 1], row):
            result.append({'time_minutes': (row['timestamp'] - start) / 60,
                           **{k: None for k in FIELDS}})
        result.append({'time_minutes': (row['timestamp'] - start) / 60,
                       **{k: row.get(k) if row.get('valid') else None for k in FIELDS}})
    return result


def plot_diagnostics(rows, title, destination):
    plt.rcParams.update({'font.size': 10, 'axes.titlesize': 11, 'axes.labelsize': 10,
                         'pdf.fonttype': 42, 'ps.fonttype': 42})
    fig, axes = plt.subplots(3, 2, figsize=(10.4, 8.2), sharex=True, sharey='row')
    arms = list(dict.fromkeys(r['arm'] for r in rows))
    for row in rows:
        col = row['run_number'] - 1
        series = row['timeseries']; x = [p['time_minutes'] for p in series]
        for r, field in enumerate(['persistent_hot_count', 'skew', 'max_partition_lag']):
            y = [np.nan if p[field] is None else p[field] for p in series]
            axes[r, col].plot(x, y, color=COLORS[row['arm']], lw=1.2,
                              drawstyle='steps-post' if r == 0 else 'default')
        y = [np.nan if p['mean_partition_lag'] is None else p['mean_partition_lag'] for p in series]
        axes[2, col].plot(x, y, color=COLORS[row['arm']], lw=1, ls='--')
    labels = ['Persistent-hot\npartition count', 'Lag skew ratio\n(maximum / mean)', 'Partition lag (offsets)\nsolid: max; dashed: mean']
    for r in range(3):
        axes[r, 0].set_ylabel(labels[r])
        for c in range(2):
            ax = axes[r, c]; ax.grid(alpha=.22); ax.axvline(1, color='#666666', ls=':', lw=1)
            ax.set_xlim(0, rows[0]['evaluation_seconds'] / 60)
            if r != 2: ax.set_ylim(bottom=-.3 if r == 0 else 0)
            else: ax.set_yscale('symlog', linthresh=1); ax.set_ylim(bottom=0)
            if r == 0:
                ax.set_title(f'Run {c + 1}')
                ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            if r == 2: ax.set_xlabel('Minutes since evaluation began')
    fig.suptitle(title, y=.985, fontsize=13)
    handles = [Line2D([0], [0], color=COLORS[a], label=LABELS[a], lw=2) for a in arms]
    fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(.54, .956), ncol=2,
               frameon=False, fontsize=9)
    fig.text(.5, .025, '700 messages/s; 60 partitions; 80% on the selected high-input partitions.\n'
             'Dotted line: scheduled action. Gaps remain missing. Bottom row: linear near zero, logarithmic above 1.',
             ha='center', fontsize=8)
    fig.subplots_adjust(top=.86, bottom=.12, left=.12, right=.98, hspace=.22, wspace=.12)
    for ext in ['pdf', 'png']: fig.savefig(destination.with_suffix('.' + ext), dpi=200)
    plt.close(fig)


def resource_plot(rows, title, destination):
    fig, axes = plt.subplots(2, 2, figsize=(10.4, 6.6))
    short = {'keep3':'Keep 3', 'redistribute3':'Redistribute 3', 'scale_redistribute6':'Targeted 6',
             'scale6':'Targeted 6', 'kafka_scale6':'Kafka 6'}
    names = [f'{short[x["arm"]]}\nRun {x["run_number"]}' for x in rows]
    keys = [('actual_cpu_core_seconds', 'Observed consumer-process CPU', 'Core-seconds'),
            ('actual_rss_gib_minutes', 'Observed consumer resident memory', 'GiB-minutes'),
            ('requested_cpu_core_minutes', 'Requested consumer CPU', 'Core-minutes'),
            ('requested_memory_gib_minutes', 'Requested consumer memory', 'GiB-minutes')]
    for ax, (key, label, units) in zip(axes.flat, keys):
        vals = [r['resources'][key] for r in rows]
        for i, (row, val) in enumerate(zip(rows, vals)):
            if val is None:
                ax.text(i, .02, 'Partial\nonly', transform=ax.get_xaxis_transform(), ha='center', fontsize=8)
            else: ax.bar(i, val, color=COLORS[row['arm']], width=.65)
        ax.set_xticks(range(len(rows)), names, rotation=25, ha='right', fontsize=7)
        ax.set_xlim(-.6, len(rows)-.4)
        ax.set_title(label, fontsize=10)
        ax.set_ylabel(units); ax.grid(axis='y', alpha=.2); ax.set_axisbelow(True)
    fig.suptitle(title, fontsize=12)
    fig.text(.5, .015, 'Evaluation + drain. Actual integrals sum covered process intervals; they are not full-cluster cost.\n'
             'Targeted 6: scale + targeted redistribution. Kafka 6: scale + Kafka rebalance. Missing request history is not zero.',
             ha='center', fontsize=8)
    fig.tight_layout(rect=[0, .08, 1, .95])
    for ext in ['pdf', 'png']: fig.savefig(destination.with_suffix('.' + ext), dpi=200)
    plt.close(fig)


def ownership_plot(rows, title, destination):
    arms = list(dict.fromkeys(r['arm'] for r in rows))
    fig, axes = plt.subplots(len(arms), 2, figsize=(10.4, 2.1*len(arms)+1), squeeze=False)
    for r in rows:
        ax = axes[arms.index(r['arm']), r['run_number']-1]
        values = r['final_high_input_partition_counts']
        highest = sum(values.values())
        for c in range(6):
            value = values.get('consumer-sts-'+str(c))
            if value is None:
                ax.text(c,.15,'n/a',ha='center',fontsize=8,color='#666666')
            else:
                ax.bar(c,value,color=COLORS[r['arm']],width=.65)
                ax.text(c,value+.1,str(value),ha='center',fontsize=9)
        ax.set_xticks(range(6),[f'C{i}' for i in range(6)])
        ax.set_ylim(0,highest+1);ax.set_xlim(-.6,5.6)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_ylabel('High-input partitions');ax.set_title(f'{LABELS[r["arm"]]} - Run {r["run_number"]}',fontsize=10)
        ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.suptitle(title,fontsize=12)
    fig.text(.5,.02,'Final valid observed ownership. All selected high-input partitions initially belonged to Consumer 2.\n'
             'Counts identify configured traffic concentration; they are not persistent-hot detector counts.',ha='center',fontsize=8)
    fig.tight_layout(rect=[0,.08,1,.95])
    for ext in ['pdf','png']:fig.savefig(destination.with_suffix('.'+ext),dpi=200)
    plt.close(fig)


def tex_table(headers, values, caption, label, widths=None):
    spec = widths or ('l' + 'r' * (len(headers) - 1))
    lines = [r'\begin{table}[!htbp]', r'\centering\small', r'\setlength{\tabcolsep}{3pt}',
             r'\renewcommand{\arraystretch}{1.12}', r'\begin{tabular}{@{}' + spec + r'@{}}',
             r'\toprule', ' & '.join(headers) + r'\\', r'\midrule']
    lines += [' & '.join(str(x) for x in row) + r'\\' for row in values]
    lines += [r'\bottomrule', r'\end{tabular}', r'\caption{' + caption + '}',
              r'\label{' + label + '}', r'\end{table}']
    return '\n'.join(lines) + '\n'


def fmt(x, digits=2):
    return '---' if x is None else f'{x:,.{digits}f}'


def figure(name, caption, label):
    return '\n'.join([r'\begin{figure}[p]', r'\centering',
                       r'\includegraphics[width=\linewidth,height=.80\textheight,keepaspectratio]{figures/' + name + '.pdf}',
                       r'\caption{' + caption + '}', r'\label{' + label + '}', r'\end{figure}', r'\clearpage']) + '\n'


def write_tables(rows, prefix, out):
    short = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute 3', 'scale6': 'Targeted scale 6',
             'scale_redistribute6': 'Targeted scale 6', 'kafka_scale6': 'Kafka scale 6'}
    outcomes = [[short[r['arm']], r['run_number'], fmt(r['p99_seconds']), fmt(r['unfinished_percent'], 3),
                 fmt(r['deadline_percent']['1000']), fmt(r['useful_throughput'])] for r in rows]
    s = tex_table(['Response', 'Run', r'\shortstack{p99\\(s)}', r'\shortstack{Unfinished\\(\%)}',
                   r'\shortstack{Misses $>1$ s\\(\%)}', r'\shortstack{Useful throughput\\(msg/s)}'], outcomes,
                  'Whole evaluation-born cohort through the fixed drain cutoff. Deadline misses include overdue unfinished messages. '
                  'Useful throughput counts distinct completions during evaluation, including warm-up records completed then; it can exceed the input rate while queued work is cleared.',
                  'tab:' + prefix + '-outcomes')
    resources = [[short[r['arm']], r['run_number'], fmt(r['resources']['actual_cpu_core_seconds'], 1),
                  fmt(r['resources']['actual_rss_gib_minutes']), fmt(r['resources']['requested_cpu_core_minutes']),
                  fmt(r['resources']['requested_memory_gib_minutes'])] for r in rows]
    s += tex_table(['Response', 'Run', r'\shortstack{Actual CPU\\(core-s)}', r'\shortstack{Actual RSS\\(GiB-min)}',
                    r'\shortstack{Requested CPU\\(core-min)}', r'\shortstack{Requested memory\\(GiB-min)}'], resources,
                   'Consumer resources over evaluation plus drain. Actual values integrate observed process CPU and RSS intervals and sum them across consumers. '
                   'Requested values integrate Kubernetes resource declarations; neither is monetary or whole-cluster cost. A dash denotes unavailable full-window request coverage, not zero cost.',
                   'tab:' + prefix + '-resources')
    diagnostics = [[short[r['arm']], r['run_number'], fmt(r['statistics']['total_lag']['time_weighted_mean'], 0),
                    fmt(r['statistics']['total_lag']['sampled_peak'], 0), fmt(100*r['statistics']['total_lag']['coverage_fraction'], 1),
                    fmt(r['statistics']['max_partition_lag']['sampled_peak'], 0),
                    fmt(r['statistics']['skew']['time_weighted_mean'])] for r in rows]
    s += tex_table(['Response', 'Run', r'\shortstack{Mean total\\lag}', r'\shortstack{Peak total\\lag}',
                    r'\shortstack{Lag coverage\\(\%)}', r'\shortstack{Peak partition\\lag}', r'Mean skew'], diagnostics,
                   'Lag diagnostics during evaluation only, in offsets except skew and coverage. Means use trapezoidal area divided by covered time; peaks are sampled maxima. '
                   'The time mean of the skew ratio is not the ratio of time-averaged maximum and mean lag. Invalid samples and ownership changes are not bridged.',
                   'tab:' + prefix + '-diagnostics')
    (out / (prefix + '-tables.tex')).write_text(s)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    ap.add_argument('--outcome-cache', type=Path)
    args = ap.parse_args(); root = args.root.resolve(); out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True); (out / 'figures').mkdir(exist_ok=True)
    cache = out / 'outcome-cache'; cache.mkdir(exist_ok=True)
    sys.path.insert(0, str(root / 'python-scripts'))
    from analyze_lag import growth_plot_lag
    from evaluate_run import evaluate
    groups = {}; hashes = {}
    for block, (record, title) in BLOCKS.items():
        source = root / 'experiment-records' / record / 'comparison.json'
        saved = read(source); hashes[str(source.relative_to(root))] = sha(source)
        rows = []
        for original in saved['runs']:
            p = root / original['raw_evidence']; run = original['run_id']
            print('Reading', block, run, flush=True)
            for name in ['manifest.json', 'outcome-summary.json', 'lag-summary.json', 'measurement-audit.json']:
                actual = sha(p / name)
                assert actual == original['evidence_hashes'][name], (run, name)
                hashes[str((p / name).relative_to(root))] = actual
            manifest = read(p / 'manifest.json'); outcome = read(p / 'outcome-summary.json')
            assert not outcome['validity_failures']
            start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
            lag = growth_plot_lag(read(p / 'lag-summary.json'), 5)
            points = [x for x in lag['snapshots'] if start <= x['timestamp'] <= end]
            for x in points:
                for key in ['persistent_hot', 'hot']:
                    x[key + '_count'] = len(x[key]) if x.get(key) is not None else None
            deadline_rows = original.get('completion_deadline_outcomes')
            if not deadline_rows:
                candidate = args.outcome_cache / (run + '-outcomes.json') if args.outcome_cache else None
                if candidate and candidate.exists():
                    replay = read(candidate); hashes['reused-outcomes/' + run] = sha(candidate)
                elif (cache / (run + '.json')).exists():
                    replay = read(cache / (run + '.json'))
                else:
                    print('Replaying deadline sensitivity', run, flush=True)
                    replay = evaluate(p, deadline_thresholds_ms=[500, 1000])
                    (cache / (run + '.json')).write_text(json.dumps(replay, indent=2))
                assert not replay['validity_failures']
                for key in ['admitted_evaluation_cohort', 'completed_by_drain', 'incomplete_by_drain',
                            'admitted_cohort_completion_p99_seconds', 'deadline_miss_fraction']:
                    assert replay[key] == outcome[key], (run, key)
                deadline_rows = replay['completion_deadline_outcomes']
            deadline_rows = [x for x in deadline_rows if x['threshold_ms'] in (500, 1000)]
            assert {x['threshold_ms'] for x in deadline_rows} == {500, 1000}
            percent = {str(int(x['threshold_ms'])): 100*x['deadline_miss_fraction'] for x in deadline_rows}
            assert percent['500'] >= percent['1000']
            request = original.get('requested_resources_evaluation_and_drain')
            if request is None:
                # Earlier campaigns kept the same resource integral under this name.
                request = original.get('requested_resources', original.get('resources', {}))
            actual_resources = {}
            for metric, key, divisor in [('consumer_cpu_percent', 'actual_cpu_core_seconds', 100),
                                          ('consumer_memory_bytes', 'actual_rss_gib_minutes', 2**30*60)]:
                values = [x['windows']['evaluation_and_drain'] for x in original['process_resources'] if x['metric'] == metric]
                actual_resources[key] = sum(x['observed_integral'] for x in values) / divisor
                actual_resources[key + '_process_coverage'] = [x['covered_fraction'] for x in values]
            full = math.isclose(request.get('covered_fraction', 0), 1, abs_tol=1e-6)
            actual_resources.update(requested_coverage=request.get('covered_fraction'),
                requested_cpu_core_minutes=request.get('consumer_container_requested_cpu_seconds_observed', 0)/60 if full else None,
                requested_memory_gib_minutes=request.get('consumer_container_requested_gib_seconds_observed', 0)/60 if full else None,
                partial_observed_request_integrals=request if not full else None)
            row = {k: original[k] for k in ['arm', 'run_number', 'run_id', 'admitted', 'completed', 'unfinished',
                                           'unfinished_percent', 'p99_seconds', 'useful_throughput']}
            row.update(block=block, evaluation_seconds=end-start, resource_seconds=manifest['drain_end_epoch']-start,
                deadline_percent=percent, deadline_counts=deadline_rows, resources=actual_resources,
                deadline_status='prespecified' if block == 'four-partition' else 'retrospective',
                completion_mean_seconds=outcome['admitted_cohort_completion_mean_seconds'],
                completion_p50_seconds=outcome['admitted_cohort_completion_p50_seconds'],
                completion_p95_seconds=outcome['admitted_cohort_completion_p95_seconds'],
                diagnostic_cohort_latencies=outcome['diagnostic_cohort_latencies'],
                completed_attempts_per_second=outcome['completed_attempts_per_second'],
                duplicate_completion_attempts=outcome['duplicate_completion_attempts'],
                observed_completion_deadline_miss_fraction=outcome['observed_completion_deadline_miss_fraction'],
                original_deadline_ms=float(manifest['config']['SLO_THRESHOLD_MS']),
                statistics={k: stats(points, k, end-start) for k in FIELDS},
                timeseries=compact_series(points, start),
                parameters=lag['parameters'], growth_plot_parameters=lag.get('growth_plot_parameters'),
                intervention_cost=original.get('intervention_cost', {}))
            row['observed_completion_deadline_percent'] = {
                str(int(x['threshold_ms'])): 100*x['observed_completion_deadline_miss_fraction']
                for x in deadline_rows}
            row['position_vs_processing_backlog_max_observed_difference'] = max(
                (abs(x['total_lag']-x['processing_backlog']) for x in points
                 if x.get('valid') and x.get('processing_backlog') is not None), default=None)
            row['final_high_input_partition_counts'] = original.get('final_hot_partitions_per_owner')
            rows.append(row)
        groups[block] = rows
        plot_diagnostics(rows, title, out / 'figures' / (block + '-diagnostics'))
        write_tables(rows, block, out)
        if block in ['twelve-partition', 'four-partition']:
            resource_plot(rows, title, out / 'figures' / (block + '-resource-cost'))
            ownership_plot(rows, title, out / 'figures' / (block + '-ownership'))
        (out / 'metrics.json').write_text(json.dumps({'groups': groups, 'source_sha256': hashes}, indent=2))
    print(f'Completed {sum(map(len, groups.values()))} trial summaries', flush=True)


if __name__ == '__main__':
    main()
