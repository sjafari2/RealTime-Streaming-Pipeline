"""Plot equal-weight means of two verified runs per response, retaining gaps."""
import argparse
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/twelve-hot600-means-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator, StrMethodFormatter
import numpy as np

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from analyze_lag import growth_plot_lag
from evidence_io import event_paths, open_events

ARMS = ['keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6']
LABELS = ['Keep 3', 'Redistribute 3', 'Scale 6 + redistribution', 'Kafka scale 6']
COLORS = ['#2563a6', '#d97b1d', '#168361', '#9653a6']


def read(path):
    return json.loads(Path(path).read_text())


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def align(series, step):
    """Align subsecond scrape jitter without filling absent or invalid values."""
    grid = np.arange(0, 602, 2) if step == 2 else np.arange(30, 601, 30)
    values = np.full(len(grid), np.nan)
    invalid = set()
    for seconds, value in series:
        nominal = round(seconds / step) * step
        if abs(nominal - seconds) > .5:
            continue
        index = round(nominal / step) - (1 if step == 30 else 0)
        if 0 <= index < len(grid):
            if value is None or not math.isfinite(value):
                invalid.add(index)
            values[index] = float(value) if index not in invalid else np.nan
    return grid, values


def pair_mean(pair, metric, step):
    if len(pair) != 2:
        raise ValueError('Exactly two independent repetitions are required')
    grid, first = align(pair[0][metric], step)
    _, second = align(pair[1][metric], step)
    values = np.array([first, second])
    valid = np.isfinite(values).all(axis=0)
    return grid, np.where(valid, values.mean(axis=0), np.nan), np.where(
        valid, values.min(axis=0), np.nan), np.where(valid, values.max(axis=0), np.nan)


def total_resource(prom, name, divisor, starts, start, end):
    """Sum all started consumers only where every component is fresh."""
    values, stamps, times = {}, {}, set()
    for series in prom:
        labels = series['metric']
        pod, metric = labels.get('pod'), labels.get('__name__')
        if pod not in starts or metric not in (name, name + '_scrape_timestamp_seconds'):
            continue
        target = values if metric == name else stamps
        if pod in target:
            raise ValueError('Ambiguous process series: ' + pod)
        target[pod] = {float(t): float(v) for t, v in series['values'] if start <= t < end}
        times.update(target[pod])
    result = []
    for t in sorted(times):
        active = [pod for pod, began in starts.items() if began <= t]
        observed = []
        for pod in active:
            value, stamp = values.get(pod, {}).get(t), stamps.get(pod, {}).get(t)
            if (value is None or stamp is None or not math.isfinite(value)
                    or value < 0 or not 0 <= t - stamp <= 10):
                observed = []
                break
            observed.append(value / divisor)
        result.append([t - start, sum(observed) if observed and len(observed) == len(active) else None])
    return result


def reconstruct(comparison_path, output):
    source = read(comparison_path)
    if sorted((r['arm'], r['run_number']) for r in source['runs']) != sorted(
            (a, n) for a in ARMS for n in (1, 2)):
        raise ValueError('Expected two runs for each of four responses')
    spec = importlib.util.spec_from_file_location('action_windows', ROOT / 'experiments/four-condition-20260929/analyze_action_windows.py')
    windows = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(windows)
    parts = output / 'verified-parts'
    parts.mkdir(parents=True, exist_ok=True)
    rows = []
    for trial in source['runs']:
        directory = ROOT / trial['raw_evidence']
        for name, expected in trial['evidence_hashes'].items():
            if digest(directory / name) != expected:
                raise ValueError('Changed evidence: ' + trial['run_id'] + '/' + name)
        signature = hashlib.sha256(json.dumps(trial, sort_keys=True).encode()).hexdigest()
        cached = parts / (trial['run_id'] + '.json')
        if cached.exists():
            row = read(cached)
            if row['source_row_sha256'] != signature:
                raise ValueError('Cached trial differs from the comparison')
            rows.append(row)
            continue
        print('Reconstructing', trial['run_id'], flush=True)
        manifest = read(directory / 'manifest.json')
        start, end, cutoff = (manifest[k] for k in ('evaluation_start_epoch', 'producer_end_epoch', 'drain_end_epoch'))
        if not math.isclose(end - start, 600):
            raise ValueError('Expected a ten-minute evaluation')
        row = dict(run_id=trial['run_id'], arm=trial['arm'], run=trial['run_number'],
                   source_row_sha256=signature, source_hashes=trial['evidence_hashes'],
                   whole_run_mean_latency_seconds=trial['mean_completion_seconds'],
                   whole_run_p99_seconds=trial['p99_seconds'], unfinished_percent=trial['unfinished_percent'])
        lag = growth_plot_lag(read(directory / 'lag-summary.json'), 15)
        for key, field in [('backlog', 'total_lag'), ('growth', 'window_growth_offsets_per_second'),
                           ('skew', 'window_mean_skew')]:
            row[key] = []
            previous = None
            for sample in lag['snapshots']:
                if not start <= sample['timestamp'] < end:
                    continue
                changed = previous is not None and sample.get('owners') != previous.get('owners')
                value = sample.get(field) if sample['valid'] and not changed else None
                row[key].append([sample['timestamp'] - start, value])
                previous = sample
        ack, done, hashes = windows.load_messages(directory, manifest, trial['intervention_cost']['event_file_sha256'])
        latencies = [v[0] - ack[k][0] for k, v in done.items() if start <= ack[k][0] < end and v[0] <= cutoff]
        assert len(latencies) == trial['completed']
        assert math.isclose(windows.p99(latencies), trial['p99_seconds'], abs_tol=1e-9)
        assert math.isclose(statistics.mean(latencies), trial['mean_completion_seconds'], rel_tol=1e-10)
        assert sum(start <= v[0] < end for v in ack.values()) == trial['admitted']
        series = windows.build_series(ack, done, start, end)
        calculated = [windows.window_metrics(series, start + t, start + t + 30) for t in range(0, 600, 30)]
        for key, field in [('throughput', 'completion_throughput_per_second'),
                           ('latency', 'completion_window_mean_seconds'), ('p99', 'completion_window_p99_seconds')]:
            row[key] = [[t + 30, w[field]] for t, w in zip(range(0, 600, 30), calculated)]
        assert math.isclose(sum(v * 30 for _, v in row['throughput']) / 600, trial['useful_throughput'], rel_tol=1e-10)
        row['event_hashes'] = hashes
        del ack, done, latencies, series
        starts = {}
        for path in event_paths(directory):
            final = read(path.parent / 'final.json')
            if final['role'] != 'consumer':
                continue
            with open_events(path) as stream:
                first = json.loads(next(stream))
            assert first['event'] == 'started' and final['pod'] not in starts
            starts[final['pod']] = first['timestamp']
        prom = read(directory / 'prometheus.json')['data']['result']
        for key, name, divisor in [('cpu', 'consumer_cpu_percent', 100), ('memory', 'consumer_memory_bytes', 2**20)]:
            row[key] = total_resource(prom, name, divisor, starts, start, end)
        row['activation'] = windows.post_action_boundary(directory, manifest, trial)
        cached.write_text(json.dumps(row, allow_nan=False) + '\n')
        rows.append(row)
        print('Verified latency, throughput and evidence:', trial['run_id'], flush=True)
    return rows


def draw(rows, panels, title, destination, columns=2):
    fig, axes = plt.subplots(math.ceil(len(panels) / columns), columns,
                             figsize=(13, 3.8 * math.ceil(len(panels) / columns) + 1.8), squeeze=False)
    for ax, (metric, heading, ylabel, step) in zip(axes.flat, panels):
        for arm, color in zip(ARMS, COLORS):
            pair = sorted([r for r in rows if r['arm'] == arm], key=lambda r: r['run'])
            grid, mean, low, high = pair_mean(pair, metric, step)
            ax.fill_between(grid / 60, low, high, color=color, alpha=.13, linewidth=0)
            ax.plot(grid / 60, mean, color=color, lw=1.8)
            if arm != 'keep3':
                for row in pair:
                    ax.axvline(row['activation']['after_evaluation_seconds'] / 60,
                               color=color, ls=':', lw=.8, alpha=.7)
        ax.axvline(1, color='#555555', ls=':', lw=1)
        if metric == 'growth':
            ax.axhline(0, color='#777777', lw=.7)
            ax.set_ylim(top=max(400, ax.get_ylim()[1]))
        else:
            ax.set_ylim(bottom=0)
        if metric == 'throughput':
            ax.axhline(600, color='#777777', lw=.8, ls='--')
        ax.set(title=heading, xlabel='Evaluation time (minutes)', ylabel=ylabel, xlim=(0, 10))
        ax.grid(alpha=.16)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=6))
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}' if metric in ('backlog', 'memory', 'throughput', 'growth') else '{x:g}'))
    for ax in list(axes.flat)[len(panels):]:
        ax.set_visible(False)
    handles = [Line2D([0], [0], color=c, lw=2, label=l) for c, l in zip(COLORS, LABELS)]
    fig.suptitle(title + '\n12 high-traffic partitions, 600 messages/s', fontsize=16, y=.98)
    fig.legend(handles=handles, ncol=4, loc='upper center', bbox_to_anchor=(.5, .90), frameon=False, fontsize=10)
    note = ('Lines: equal-weight mean of two runs. Bands: observed two-run range, not confidence intervals.\n'
            'Dotted lines: gray = action request at 1 minute, colors = confirmed activation in each run.\n'
            'Only evaluation is shown. Missing observations remain gaps. Both runs must be valid at each plotted time.')
    if any(p[0] == 'throughput' for p in panels):
        note += '\nLatency: completed evaluation messages in each 30-second interval. Throughput includes completed warm-up work. Dashed line = 600 msg/s.'
    fig.text(.5, .02, note, ha='center', va='bottom', fontsize=9)
    fig.subplots_adjust(left=.08, right=.98, top=.81, bottom=.17 if len(panels) > 2 else .24, wspace=.25, hspace=.38)
    for extension in ('png', 'pdf'):
        fig.savefig(destination.with_suffix('.' + extension), dpi=180, facecolor='white')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    rows = reconstruct(args.comparison, args.output)
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False})
    draw(rows, [('backlog', 'Total backlog', 'Offsets', 2),
                ('throughput', 'Throughput', 'Messages/s', 30),
                ('latency', 'Mean end-to-end latency in each interval', 'Seconds', 30),
                ('p99', 'P99 end-to-end latency in each interval', 'Seconds', 30)],
         'Performance evaluation metrics', args.output / 'mean-performance')
    draw(rows, [('growth', 'Backlog growth rate (30-second rolling window)', 'Offsets/s', 2),
                ('skew', 'Lag skew ratio (15-observation rolling mean)', 'Maximum / mean partition lag', 2)],
         'Runtime control signals', args.output / 'mean-control-signals')
    draw(rows, [('cpu', 'Actual CPU use across consumers', 'CPU cores', 2),
                ('memory', 'Actual RSS memory across consumers', 'MiB', 2)],
         'Actual resource use', args.output / 'mean-resources')
    definitions = dict(averaging='Equal-weight mean of two runs at the same evaluation time. A mean is unavailable if either run is unavailable.',
        uncertainty='Bands show the range of the two observations, not a confidence interval.',
        backlog='Total consumer-position lag, summed across partitions, in offsets.',
        growth='Change in total backlog across 15 valid consecutive intervals divided by elapsed time, nominally 30 seconds. Not a ratio.',
        skew='Per-run arithmetic mean of 15 valid lag skew ratios, then the mean of those values across two runs.',
        resources='Actual CPU cores and RSS MiB summed across all started consumers before averaging the two runs. Missing/stale components invalidate the total.',
        latency='Mean original creation-to-completion latency of evaluation messages completed in each nonoverlapping 30-second interval, then equal-weight mean of the two runs.',
        p99='Mean of the two run-level interval p99 values. Not the p99 of pooled messages.',
        throughput='Distinct completions per 30-second interval, including warm-up messages completed during evaluation.',
        scope='Time-series plots exclude drain. Whole-run cohort mean and p99 follow evaluation messages through the drain cutoff.')
    payload = dict(comparison_sha256=digest(args.comparison), script_sha256=digest(__file__), definitions=definitions, rows=rows)
    (args.output / 'mean-plot-data.json.gz').write_bytes(gzip.compress(json.dumps(payload, allow_nan=False).encode(), mtime=0))
    print('Created and validated three mean-comparison figures for eight trials.', flush=True)


if __name__ == '__main__':
    main()
