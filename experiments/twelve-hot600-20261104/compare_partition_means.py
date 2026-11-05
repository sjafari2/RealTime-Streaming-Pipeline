"""Compare four- and twelve-partition repeat means without pooling deployments."""
import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'experiments/twelve-hot600-20261104'))
from plot_repeat_means import (ARMS, LABELS, COLORS, digest, read, pair_mean,
                               growth_plot_lag, plt, np, Line2D, MaxNLocator, StrMethodFormatter)
from matplotlib.ticker import MultipleLocator


def load_compressed(path):
    return json.loads(gzip.decompress(Path(path).read_bytes()))


def require_hash(path, expected):
    if digest(path) != expected:
        raise ValueError('Source hash mismatch: ' + str(path))


def select_intervals(rolling):
    """Select original nonoverlapping intervals, not means of overlapping windows."""
    selected = []
    for end in range(30, 601, 30):
        matches = [r for r in rolling if math.isclose(r['end_after_evaluation_seconds'], end, abs_tol=1e-6)]
        if len(matches) != 1 or not math.isclose(matches[0]['duration_seconds'], 30):
            raise ValueError('Missing or ambiguous 30-second completion interval')
        selected.append(matches[0])
    return selected


def check_series(left, right):
    if len(left) != len(right):
        raise ValueError('Different observation counts')
    for (ta, a), (tb, b) in zip(left, right):
        if not math.isclose(ta, tb, abs_tol=1e-6):
            raise ValueError('Observation times differ')
        if (a is None) != (b is None) or (a is not None and not math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-9)):
            raise ValueError('Observation values differ')


def lag_fields(directory, trial):
    for name in ('manifest.json', 'lag-summary.json'):
        require_hash(directory / name, trial['evidence_hashes'][name])
    manifest = read(directory / 'manifest.json')
    start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
    lag = growth_plot_lag(read(directory / 'lag-summary.json'), 15)
    result = {}
    for key, field in [('backlog', 'total_lag'), ('growth', 'window_growth_offsets_per_second'), ('skew', 'window_mean_skew')]:
        result[key] = []
        previous = None
        for sample in lag['snapshots']:
            if not start <= sample['timestamp'] < end:
                continue
            changed = previous is not None and sample.get('owners') != previous.get('owners')
            value = sample.get(field) if sample['valid'] and not changed else None
            result[key].append([sample['timestamp'] - start, value])
            previous = sample
    return result, manifest


def four_rows(comparison, cache, completion_path, resource_path):
    windows, resources = read(completion_path), read(resource_path)
    rows = []
    for trial in comparison['runs']:
        cached = next(r for r in cache['rows'] if r['run_id'] == trial['run_id'])
        require_hash(completion_path, cached['completion_summary']['sha256'])
        require_hash(resource_path, cached['resource_summary']['sha256'])
        signature = hashlib.sha256(json.dumps(trial, sort_keys=True).encode()).hexdigest()
        if cached['comparison_row_sha256'] != signature:
            raise ValueError('Saved comparison row differs from cached evidence')
        w = next(r for r in windows['runs'] if r['run_id'] == trial['run_id'])
        if not w['whole_run_replay_matches'] or w['event_file_sha256'] != trial['intervention_cost']['event_file_sha256']:
            raise ValueError('Completion replay has not verified this run')
        if w['evidence_sha256'] != trial['evidence_hashes']:
            raise ValueError('Completion and comparison evidence differ')
        row = dict(run_id=trial['run_id'], arm=trial['arm'], run=trial['run_number'], block='four',
                   source_hashes=trial['evidence_hashes'], source_row_sha256=signature)
        lag, manifest = lag_fields(ROOT / trial['raw_evidence'], trial)
        row.update(lag)
        row['placement'] = [{k: p[k] for k in ('role', 'pod', 'node', 'resources')} for p in manifest['initial_node_placement']]
        selected = select_intervals(w['rolling'])
        for key, field in [('throughput', 'completion_throughput_per_second'),
                           ('latency', 'completion_window_mean_seconds'), ('p99', 'completion_window_p99_seconds')]:
            row[key] = [[t, v[field]] for t, v in zip(range(30, 601, 30), selected)]
        check_series(row['throughput'], cached['throughput'])
        check_series(row['p99'], cached['window_p99'])
        if not math.isclose(sum(v for _, v in row['throughput']) / 20, trial['useful_throughput'], rel_tol=1e-10):
            raise ValueError('Window throughput does not reproduce the run result')
        end = manifest['producer_end_epoch'] - manifest['evaluation_start_epoch']
        for key, metric in [('cpu', 'consumer_cpu_percent'), ('memory', 'consumer_memory_bytes')]:
            original = resources[trial['run_id']][metric]
            trace = [[t * 60, v] for t, v in zip(original['minutes'], original['values'])]
            check_series(trace, cached[key])
            row[key] = [[t, v] for t, v in trace if t < end]
        if w['post_action_boundary'] != cached['activation']:
            raise ValueError('Activation records differ')
        row['activation'] = w['post_action_boundary']
        rows.append(row)
        print('Verified four-partition run:', trial['run_id'], flush=True)
    return rows


def twelve_rows(comparison, data, comparison_path):
    if data['comparison_sha256'] != digest(comparison_path):
        raise ValueError('Twelve-partition traces use another comparison')
    result = []
    for trial in comparison['runs']:
        row = dict(next(r for r in data['rows'] if r['run_id'] == trial['run_id']))
        signature = hashlib.sha256(json.dumps(trial, sort_keys=True).encode()).hexdigest()
        if row['source_row_sha256'] != signature or row['source_hashes'] != trial['evidence_hashes']:
            raise ValueError('Twelve-partition source records differ')
        manifest_path = ROOT / trial['raw_evidence'] / 'manifest.json'
        require_hash(manifest_path, trial['evidence_hashes']['manifest.json'])
        manifest = read(manifest_path)
        row['placement'] = [{k: p[k] for k in ('role', 'pod', 'node', 'resources')} for p in manifest['initial_node_placement']]
        row['block'] = 'twelve'
        result.append(row)
    return result


def time_mean(series):
    """Integrate complete neighboring resource observations, without filling gaps."""
    area = covered = 0.
    for (t0, v0), (t1, v1) in zip(series, series[1:]):
        if v0 is not None and v1 is not None and 0 < t1 - t0 <= 3:
            area += (v0 + v1) / 2 * (t1 - t0)
            covered += t1 - t0
    return dict(mean=area / covered if covered else None, covered_seconds=covered,
                coverage_fraction=covered / 600)


def summarize(rows, comparisons):
    result = []
    for block, comparison in comparisons.items():
        for arm in ARMS:
            pair = sorted([r for r in rows if r['block'] == block and r['arm'] == arm], key=lambda r: r['run'])
            trials = sorted([r for r in comparison['runs'] if r['arm'] == arm], key=lambda r: r['run_number'])
            assert len(pair) == len(trials) == 2
            metrics = {}
            for label, field in [('mean_end_to_end_latency_seconds', 'mean_completion_seconds'),
                                 ('p99_end_to_end_latency_seconds', 'p99_seconds'), ('unfinished_percent', 'unfinished_percent'),
                                 ('throughput_messages_per_second', 'useful_throughput'), ('mean_total_backlog_offsets', 'mean_lag'),
                                 ('mean_lag_skew_ratio', 'mean_skew')]:
                metrics[label] = [r[field] for r in trials]
            resources = {}
            for key in ('cpu', 'memory'):
                resources[key] = [time_mean(r[key]) for r in pair]
                metrics['mean_actual_' + key] = [r['mean'] for r in resources[key]]
            metrics['mean_rolling_backlog_growth_offsets_per_second'] = [statistics.mean(v for _, v in r['growth'] if v is not None) for r in pair]
            values = {key: dict(individual=v, mean=statistics.mean(v), sample_standard_deviation=statistics.stdev(v)) for key, v in metrics.items()}
            result.append(dict(block=block, arm=arm, run_ids=[r['run_id'] for r in pair], metrics=values, resource_coverage=resources))
    return result


def draw(rows, panels, category, destination):
    nrows = len(panels)
    fig, axes = plt.subplots(nrows, 2, figsize=(13.2, 3.0 * nrows + 2), sharex=True, sharey='row', squeeze=False)
    for col, block in enumerate(('twelve', 'four')):
        subset = [r for r in rows if r['block'] == block]
        for i, (metric, label, step) in enumerate(panels):
            ax = axes[i, col]
            for arm, color in zip(ARMS, COLORS):
                pair = sorted([r for r in subset if r['arm'] == arm], key=lambda r: r['run'])
                grid, mean, low, high = pair_mean(pair, metric, step)
                ax.fill_between(grid / 60, low, high, color=color, alpha=.13, linewidth=0)
                ax.plot(grid / 60, mean, color=color, lw=1.7)
                if arm != 'keep3':
                    for r in pair:
                        ax.axvline(r['activation']['after_evaluation_seconds'] / 60, color=color, ls=':', lw=.7, alpha=.7)
            ax.axvline(1, color='#555555', ls=':', lw=.9)
            if metric == 'growth':
                ax.axhline(0, color='#777777', lw=.7)
            if metric == 'throughput':
                ax.axhline(600, color='#777777', lw=.8, ls='--')
            ax.set_xlim(0, 10)
            ax.grid(alpha=.16)
            ax.tick_params(labelbottom=True, labelleft=True)
            ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
            ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}' if metric in ('backlog', 'memory', 'throughput', 'growth') else '{x:g}'))
            ax.set_ylabel(label)
            if i == 0:
                ax.set_title('12 high-traffic partitions' if block == 'twelve' else '4 high-traffic partitions', fontsize=13, pad=12, fontweight='bold')
            if i == nrows - 1:
                ax.set_xlabel('Evaluation time (minutes)')
    for i, (metric, _, _) in enumerate(panels):
        if metric == 'growth':
            axes[i, 0].set_ylim(top=max(400, axes[i, 0].get_ylim()[1]))
            axes[i, 0].yaxis.set_major_locator(MultipleLocator(400))
        else:
            axes[i, 0].set_ylim(bottom=0)
    handles = [Line2D([0], [0], color=c, lw=2, label=l) for c, l in zip(COLORS, LABELS)]
    fig.suptitle(category + '\n600 messages/s total, mean of two runs per method', fontsize=16, y=.985)
    fig.legend(handles=handles, ncol=4, loc='upper center', bbox_to_anchor=(.5, .925 if nrows > 2 else .89), frameon=False, fontsize=10)
    note = ('Lines: equal-weight means. Bands: two-run range, not confidence intervals. Both runs must be valid at each time.\n'
            'Dotted lines: gray = request at minute 1, colors = each run\'s confirmed activation. Gaps remain unavailable.\n'
            'Same input rate, separate deployment blocks. Differences do not isolate the effect of traffic concentration.')
    if any(p[0] == 'latency' for p in panels):
        note += '\nLatency uses completed evaluation messages in 30-second intervals. Throughput includes completed warm-up work. Dashed line = 600 msg/s.'
    elif any(p[0] == 'cpu' for p in panels):
        note += '\nActual CPU and RSS totals across started consumers. Evaluation only, excluding warm-up and drain.'
    else:
        note += '\nGrowth: 15 two-second intervals. Skew: mean of 15 observations. Ownership changes restart affected rolling windows.'
    fig.text(.5, .018, note, fontsize=9, va='bottom', ha='center')
    fig.subplots_adjust(left=.09, right=.97, top=.855 if nrows > 2 else .78,
                        bottom=.115 if nrows > 2 else .20, hspace=.20, wspace=.24)
    for extension in ('png', 'pdf'):
        fig.savefig(destination.with_suffix('.' + extension), dpi=180, facecolor='white')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('four-comparison', 'four-cache', 'four-windows', 'four-resources', 'twelve-comparison', 'twelve-data', 'output'):
        parser.add_argument('--' + key, type=Path, required=True)
    a = parser.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    comparisons = dict(four=read(a.four_comparison), twelve=read(a.twelve_comparison))
    windows = read(a.four_windows)
    if windows['comparison_sha256'] != digest(a.four_comparison):
        raise ValueError('Four-partition windows do not match the comparison')
    for block, comparison in comparisons.items():
        if comparison['cost_protocol']['aggregate_input_rate'] != 600:
            raise ValueError('Both blocks must have 600 messages/s total input')
        expected = 4 if block == 'four' else 12
        if len(comparison['hot_partitions']) != expected:
            raise ValueError('Unexpected number of high-traffic partitions')
    rows = four_rows(comparisons['four'], load_compressed(a.four_cache), a.four_windows, a.four_resources)
    rows += twelve_rows(comparisons['twelve'], load_compressed(a.twelve_data), a.twelve_comparison)
    assert len(rows) == 16 and len({r['run_id'] for r in rows}) == 16
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False})
    draw(rows, [('backlog', 'Total backlog (offsets)', 2), ('throughput', 'Throughput (messages/s)', 30),
                ('latency', 'Mean end-to-end latency (s)\nin each 30-second interval', 30),
                ('p99', 'P99 end-to-end latency (s)\nin each 30-second interval', 30)],
         'Performance evaluation metrics', a.output / 'performance-12-vs-4')
    draw(rows, [('growth', 'Backlog growth rate (offsets/s)\n30-second rolling window', 2),
                ('skew', 'Lag skew ratio\n15-observation rolling mean', 2)],
         'Runtime control signals', a.output / 'control-signals-12-vs-4')
    draw(rows, [('cpu', 'Actual consumer CPU use (cores)', 2), ('memory', 'Actual consumer RSS memory (MiB)', 2)],
         'Resource cost metrics: actual use', a.output / 'resource-use-12-vs-4')
    summaries = summarize(rows, comparisons)
    definitions = dict(time_series='Each time-series point is the equal-weight mean of two runs at the same evaluation time, requiring valid values in both.',
        bands='Observed two-run range, not confidence intervals.',
        p99='Run-level and window p99 averages are means of percentiles, not percentiles of pooled messages.',
        whole_run_latency='Mean and p99 for completed evaluation messages followed through drain, reported with unfinished percentage.',
        resources='Run summary means integrate aggregate consumer CPU/RSS over adjacent valid evaluation observations. Coverage is reported. Warm-up and drain are excluded.',
        backlog_and_skew_summary='Arithmetic means of valid original evaluation observations within each run, then equal weight across the two runs.',
        growth='Arithmetic mean of valid 30-second rolling total-backlog growth values, then equal weight across the two runs.',
        scope='Performance and control traces exclude drain. CPU/RSS represent actual process use, not requests or monetary costs.',
        comparison='Both blocks use 600 messages/s and 60 partitions with 80% of input concentrated. Different deployments and starting ownership prevent attributing differences to partition count alone.')
    provenance = {k: digest(v) for k, v in vars(a).items() if k != 'output'}
    payload = dict(source_sha256=provenance, script_sha256=digest(__file__), definitions=definitions, rows=rows)
    (a.output / 'comparison-plot-data.json.gz').write_bytes(gzip.compress(json.dumps(payload, allow_nan=False).encode(), mtime=0))
    (a.output / 'mean-results.json').write_text(json.dumps(dict(definitions=definitions, summaries=summaries), indent=2, allow_nan=False) + '\n')
    print('Validated all 16 runs and generated three side-by-side category figures.', flush=True)


if __name__ == '__main__':
    main()
