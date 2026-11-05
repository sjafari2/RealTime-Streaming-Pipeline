"""Replay a balanced reference run and plot its twenty-minute evaluation."""
import argparse
import gzip
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/matched-calibration-mpl')
sys.path.insert(0, str(ROOT / 'experiments/twelve-hot600-20261104'))
from plot_repeat_means import read, digest, total_resource, event_paths, open_events, growth_plot_lag, plt, np
from compare_partition_means import time_mean


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def line(ax, values, label=None, color='#2563a6', **options):
    ax.plot([t / 60 for t, v in values], [np.nan if v is None else v for t, v in values],
            color=color, lw=1.4, label=label, **options)


def figure(series, title, panels, destination):
    fig, axes = plt.subplots(len(panels), 1, figsize=(10.2, 2.25 * len(panels) + 1.3),
                             sharex=True, squeeze=False)
    for ax, (metric, label) in zip(axes[:, 0], panels):
        line(ax, series[metric])
        if metric == 'latency':
            ax.lines[0].set_label('Mean')
            line(ax, series['p99'], label='p99', color='#ad4c81')
            ax.axhline(1, color='#888888', ls='--', lw=.85, label='1-second SLO')
            ax.legend(loc='upper right', ncol=3, frameon=False, fontsize=9)
        elif metric == 'throughput':
            ax.axhline(600, color='#777777', ls='--', lw=.85, label='600 msg/s target')
            ax.legend(loc='lower right', frameon=False, fontsize=9)
        elif metric == 'growth':
            ax.axhline(0, color='#777777', lw=.85)
        if metric != 'growth':
            upper = max(v for _, v in series[metric] if v is not None)
            if metric == 'latency':
                upper = max(1, upper, max(v for _, v in series['p99'] if v is not None))
            elif metric == 'throughput':
                upper = max(600, upper)
            ax.set_ylim(0, max(1, upper * 1.08))
        ax.set_ylabel(label)
        ax.set_xlim(0, 20)
        ax.set_xticks(np.arange(0, 21, 2))
        ax.grid(alpha=.2)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    axes[-1, 0].set_xlabel('Evaluation time (min)')
    fig.suptitle(title + '\nBalanced input at 600 msg/s, 3 consumers, one run', fontsize=14, y=.98)
    note = 'Evaluation only. Warm-up (1 min) and drain (2 min) are excluded from these plots. No intervention.'
    if panels[0][0] == 'backlog':
        note += '\nThroughput and latency use 30-second completion windows. Latency uses completed evaluation messages.\nThroughput can include warm-up messages completed during evaluation. This window p99 differs from the run-level p99.'
    elif panels[0][0] == 'growth':
        note += '\nGrowth uses 15 consecutive differences (30 s). Skew uses 15 observations (28 s). Missing values remain gaps.'
    else:
        note += '\nCPU and RSS memory are summed across all 3 consumer processes. Only fresh observations are used.'
    fig.text(.5, .015, note, ha='center', va='bottom', fontsize=8.4)
    fig.subplots_adjust(left=.13, right=.97, top=.86 if len(panels) == 3 else .82,
                        bottom=.16 if len(panels) == 3 else .2, hspace=.22)
    fig.savefig(destination, dpi=160, facecolor='white')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    d, out = args.run_directory, args.output
    out.mkdir(parents=True, exist_ok=True)
    manifest, outcome = read(d / 'manifest.json'), read(d / 'outcome-summary.json')
    assert read(d / 'runner-status.json')['status'] == 'complete'
    assert not outcome['validity_failures']
    assert manifest['config']['TRAFFIC_MODE'] == 'balanced'
    assert manifest['intervention']['action'] == 'none'
    assert len(manifest['consumer_pods']) == len(manifest['producer_pods']) == 3
    assert manifest['config']['NUM_PARTITIONS'] == '60'
    start, end, cutoff = (manifest[k] for k in ('evaluation_start_epoch', 'producer_end_epoch', 'drain_end_epoch'))
    assert math.isclose(end - start, 1200) and math.isclose(cutoff - end, 120)
    assert math.isclose(start - manifest['start_epoch'], 60)
    checks = [json.loads(row) for row in (d / 'matched-placement-checks.jsonl').read_text().splitlines()]
    assert checks and all(not row['issues'] for row in checks)
    counts = {c: sum(p['pod'] == c for p in manifest['initial_assignment']) for c in manifest['consumer_pods']}
    assert set(counts.values()) == {20}
    windows = load(ROOT / 'experiments/four-condition-20260929/analyze_action_windows.py', 'calibration_windows')
    compression = read(d / 'compressed-evidence.json')
    expected = {f['compressed_path']: f['compressed_sha256'] for f in compression['files']
                if f['compressed_path'].endswith('events.jsonl.gz')}
    print('Replaying verified message records', flush=True)
    ack, done, hashes = windows.load_messages(d, manifest, expected)
    completed = [v[0] - ack[k][0] for k, v in done.items() if start <= ack[k][0] < end and v[0] <= cutoff]
    assert sum(start <= v[0] < end for v in ack.values()) == outcome['admitted_evaluation_cohort']
    assert len(completed) == outcome['completed_by_drain']
    assert math.isclose(windows.p99(completed), outcome['admitted_cohort_completion_p99_seconds'], abs_tol=1e-9)
    assert math.isclose(statistics.mean(completed), outcome['admitted_cohort_completion_mean_seconds'], rel_tol=1e-10)
    raw = windows.build_series(ack, done, start, end)
    bins = [windows.window_metrics(raw, start + t, start + t + 30) for t in range(0, 1200, 30)]
    series = {}
    for key, field in [('throughput', 'completion_throughput_per_second'), ('latency', 'completion_window_mean_seconds'), ('p99', 'completion_window_p99_seconds')]:
        series[key] = [[t + 30, row[field]] for t, row in zip(range(0, 1200, 30), bins)]
    assert math.isclose(statistics.mean(v for _, v in series['throughput']), outcome['useful_throughput_per_second'], rel_tol=1e-10)
    del ack, done, completed, raw
    lag = growth_plot_lag(read(d / 'lag-summary.json'), 15)
    for key, field in [('backlog', 'total_lag'), ('processing_backlog', 'processing_backlog'),
                       ('growth', 'window_growth_offsets_per_second'), ('skew', 'window_mean_skew')]:
        series[key] = [[r['timestamp'] - start, r.get(field) if r['valid'] else None]
                       for r in lag['snapshots'] if start <= r['timestamp'] < end]
    starts = {}
    for path in event_paths(d):
        final = read(path.parent / 'final.json')
        if final['role'] == 'consumer':
            with open_events(path) as stream:
                first = json.loads(next(stream))
            assert first['event'] == 'started'
            starts[final['pod']] = first['timestamp']
    assert set(starts) == set(manifest['consumer_pods'])
    prom = read(d / 'prometheus.json')['data']['result']
    for key, name, divisor in [('cpu', 'consumer_cpu_percent', 100), ('memory', 'consumer_memory_bytes', 2**20)]:
        series[key] = total_resource(prom, name, divisor, starts, start, end)
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10.5})
    figure(series, 'Performance evaluation metrics', [('backlog', 'Total backlog (offsets)'),
        ('throughput', 'Throughput (msg/s)'), ('latency', 'Latency (s)')], out / 'balanced-performance.png')
    figure(series, 'Runtime control signals', [('growth', 'Backlog growth (offsets/s)'),
        ('skew', 'Lag skew ratio')], out / 'balanced-control-signals.png')
    figure(series, 'Resource use', [('cpu', 'CPU use (cores)'),
        ('memory', 'Memory use (MiB)')], out / 'balanced-resource-use.png')
    summary = dict(run_id=manifest['run_id'], initial_partition_counts=counts,
        evaluation_seconds=end-start, admitted=outcome['admitted_evaluation_cohort'], completed=outcome['completed_by_drain'],
        unfinished=outcome['incomplete_by_drain'], input_rate=outcome['admitted_messages_per_second'],
        throughput=outcome['useful_throughput_per_second'], whole_run_mean_latency_seconds=outcome['admitted_cohort_completion_mean_seconds'],
        whole_run_p99_seconds=outcome['admitted_cohort_completion_p99_seconds'], deadline_miss_fraction=outcome['deadline_miss_fraction'],
        peak_total_backlog=max(v for _, v in series['backlog'] if v is not None),
        consumer_resource_means={k: time_mean(series[k]) for k in ('cpu', 'memory')},
        lag_coverage=lag['covered_fraction'], replay_matches_saved_outcomes=True,
        source_script_sha256=digest(Path(__file__)),
        analysis_base_revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        event_file_sha256=hashes,
        evidence_sha256={name: digest(d/name) for name in ['manifest.json', 'outcome-summary.json', 'lag-summary.json',
            'prometheus.json', 'matched-placement-checks.jsonl', 'compressed-evidence.json']},
        definitions=dict(whole_run='Evaluation messages followed through drain. Latency is reported only for completed messages.',
            throughput='Distinct completions during each evaluation interval, including warm-up messages completed then.',
            p99='Nearest-rank p99 of evaluation-message latencies completed in each 30-second interval, measured from original producer timestamps.',
            growth='Mean of 15 consecutive lag changes divided by elapsed time, using valid two-second observations.',
            skew='Mean of 15 consecutive valid max-to-mean partition-lag ratios.',
            resources='Total observed consumer CPU cores and RSS memory. Time-weighted means over valid evaluation intervals.'))
    # The shared resource helper reports ten-minute coverage. This run lasts twenty minutes.
    for value in summary['consumer_resource_means'].values():
        value['coverage_fraction'] = value['covered_seconds'] / (end-start)
    save(out/'summary.json', summary)
    with gzip.open(out/'plot-data.json.gz', 'wt') as stream:
        json.dump(dict(run_id=manifest['run_id'], series=series), stream, allow_nan=False)
    print(json.dumps({k:summary[k] for k in ['run_id', 'admitted', 'completed', 'unfinished', 'peak_total_backlog', 'consumer_resource_means']}, indent=2), flush=True)


if __name__ == '__main__':
    main()
