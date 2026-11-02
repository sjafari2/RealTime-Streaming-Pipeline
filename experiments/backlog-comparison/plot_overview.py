"""Plot the completed starting-backlog trials with both runs shown separately."""

import argparse
import hashlib
import importlib.util
import json
import math
import os
import subprocess
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/stream-plot-cache')

ARMS = ['keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6']
LABELS = ['Keep 3', 'Redistribute 3', 'Targeted scale 6', 'Kafka scale 6']
COLORS = ['#2563a6', '#dc862d', '#21875e', '#9a4eaa']
CONDITIONS = ['smaller', 'larger']


def read(path):
    raw = path.read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def collect(repo):
    record = repo / 'experiment-records/backlog-comparison-20261027'
    progress, progress_hash = read(record / 'progress.json')
    comparison, comparison_hash = read(
        repo / 'results/four-condition-20261027-003002-analysis/comparison.json')
    assert progress['status'] == comparison['status'] == 'complete'
    assert progress['completed_trials'] == comparison['completed_trials'] == 16
    originals = {r['run_id']: r for r in comparison['runs']}
    rows = []
    combinations = set()
    for row in progress['runs']:
        source = originals[row['run_id']]
        for key in ['backlog_condition', 'arm', 'run_number', 'admitted',
                    'completed', 'unfinished', 'unfinished_percent', 'p99_seconds',
                    'recovery_common_decision',
                    'requested_resources_evaluation_and_drain']:
            assert row[key] == source[key], (row['run_id'], key)
        condition, arm, run = row['backlog_condition'], row['arm'], row['run_number']
        assert row['admitted'] == row['completed'] + row['unfinished'] == 360000
        assert math.isclose(row['unfinished_percent'],
                            100 * row['unfinished'] / row['admitted'])
        combinations.add((condition, arm, run))
        directory = repo / 'results' / row['run_id']
        manifest, manifest_hash = read(directory / 'manifest.json')
        lag, lag_hash = read(directory / 'lag-summary.json')
        assert manifest_hash == row['evidence_hashes']['manifest.json']
        assert lag_hash == row['evidence_hashes']['lag-summary.json']
        start = manifest['evaluation_start_epoch']
        assert math.isclose(manifest['producer_end_epoch'] - start, 600)
        assert math.isclose(manifest['drain_end_epoch'] - start, 720)
        points = []
        rolling = {'growth': [], 'skew': []}
        assert lag['parameters']['window_samples'] == 15
        previous = None
        for sample in lag['snapshots']:
            t = sample['timestamp'] - start
            if t < -0.01 or t > 600.01:
                continue
            value = sample.get('processing_backlog')
            valid = sample['valid'] and value is not None
            if previous is not None and valid:
                if (sample['timestamp'] - previous['timestamp'] > 3 or
                        sample.get('owners') != previous.get('owners')):
                    points.append([max(t, 0), None])
            points.append([max(t, 0), value if valid else None])
            for name, key in [('growth', 'window_processing_backlog_growth_offsets_per_second'),
                              ('skew', 'window_mean_skew')]:
                value = sample.get(key) if valid else None
                if previous is not None and valid and (
                        sample['timestamp'] - previous['timestamp'] > 3 or
                        sample.get('owners') != previous.get('owners')):
                    rolling[name].append([max(t, 0), None])
                rolling[name].append([max(t, 0), value])
            previous = sample if valid else None
        resources = row['requested_resources_evaluation_and_drain']
        assert math.isclose(resources['covered_fraction'], 1)
        cpu = resources['consumer_container_requested_cpu_seconds_observed'] / 60
        memory = resources['consumer_container_requested_gib_seconds_observed'] / 60
        # Every consumer requests 2 cores and 0.75 GiB in this campaign.
        assert math.isclose(memory / cpu, 0.375, abs_tol=1e-9)
        recovery = row['recovery_common_decision']['confirmation_after_anchor_seconds']
        activation = source['activation']['after_evaluation_seconds'] - 60
        rows.append(dict(
            run_id=row['run_id'], condition=condition, arm=arm, run=run,
            p99_seconds=row['p99_seconds'], unfinished_percent=row['unfinished_percent'],
            admitted=row['admitted'], completed=row['completed'], unfinished=row['unfinished'],
            requested_cpu_core_minutes=cpu, requested_memory_gib_minutes=memory,
            activation_after_decision_seconds=activation,
            recovery_after_decision_seconds=recovery,
            processing_backlog=points,
            rolling=rolling,
            source_hashes={'manifest.json': manifest_hash, 'lag-summary.json': lag_hash}))
    assert combinations == {(c, a, r) for c in CONDITIONS for a in ARMS for r in [1, 2]}
    return dict(
        description='16 completed Kafka trials; separate runs, no averaging or imputation',
        analysis_code_revision=subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
        source_hashes={'progress.json': progress_hash, 'comparison.json': comparison_hash},
        definitions={
            'processing_backlog': 'Raw valid Q(t) observations during evaluation; gaps and ownership changes break lines.',
            'p99': 'Evaluation messages completed by the drain cutoff, creation to application completion; unfinished percentage retains all acknowledged evaluation messages.',
            'resources': 'Requested consumer resources integrated over evaluation and drain (720 seconds), excluding warm-up.',
            'recovery': 'Seconds from the scheduled decision to confirmation of Q <= 100 offsets for 30 continuous valid seconds during input.',
            'activation': 'Seconds from scheduled decision to verified resulting assignment; this is not a processing pause.'},
        rows=rows)


def add_completion_windows(repo, data, cache):
    """Use the existing message reconciliation and interval definitions."""
    module_path = repo / 'experiments/four-condition-20260929/analyze_action_windows.py'
    spec = importlib.util.spec_from_file_location('backlog_completion_windows', module_path)
    windows = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(windows)
    comparison, _ = read(repo / 'results/four-condition-20261027-003002-analysis/comparison.json')
    sources = {row['run_id']: row for row in comparison['runs']}
    cache.mkdir(parents=True, exist_ok=True)
    for row in data['rows']:
        source = sources[row['run_id']]
        cached = cache / (row['run_id'] + '.json')
        expected = source['intervention_cost']['event_file_sha256']
        if cached.exists():
            values, _ = read(cached)
            assert values['message_evidence_sha256'] == expected
            assert values['manifest_sha256'] == row['source_hashes']['manifest.json']
        else:
            directory = repo / 'results' / row['run_id']
            manifest, _ = read(directory / 'manifest.json')
            start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
            ack, done, hashes = windows.load_messages(directory, manifest, expected)
            series = windows.build_series(ack, done, start, end)
            cohort_latencies = [value[0] - ack[key][0] for key, value in done.items()
                               if start <= ack[key][0] < end
                               and value[0] <= manifest['drain_end_epoch']]
            assert len(cohort_latencies) == row['completed']
            assert math.isclose(windows.p99(cohort_latencies), row['p99_seconds'], abs_tol=1e-9)
            for left, right, key in [(start+30, start+60, 'before'), (end-30, end, 'evaluation_end')]:
                measured = windows.window_metrics(series, left, right)
                for metric in ['distinct_completions', 'completion_window_p99_seconds']:
                    assert measured[metric] == source['windows'][key]['completion'][metric]
            throughput = [windows.window_metrics(series, start+t, start+t+10)
                          for t in range(0, 600, 10)]
            p99 = [windows.window_metrics(series, start+t, start+t+30)
                   for t in range(0, 600, 30)]
            active = source['activation']['epoch']
            values = dict(
                manifest_sha256=row['source_hashes']['manifest.json'], message_evidence_sha256=hashes,
                throughput=[[w['end_epoch']-start, w['completion_throughput_per_second']] for w in throughput],
                window_p99=[[w['end_epoch']-start, w['completion_window_p99_seconds']] for w in p99],
                first_30_seconds_after_activation=windows.window_metrics(series, active, active+30))
            cached.write_text(json.dumps(values, indent=2, allow_nan=False)+'\n')
            del ack, done, series, cohort_latencies
        row.update(values)
        print('Verified completion windows:', row['condition'], row['arm'], row['run'], flush=True)
    data['definitions']['throughput'] = 'Distinct first completions per 10-second interval, including warm-up messages completed during evaluation.'
    data['definitions']['window_p99'] = 'Nearest-rank p99 of full end-to-end latencies for evaluation messages completed within each nonoverlapping 30-second interval; plotted at interval end, excluding drain.'
    data['definitions']['rolling'] = 'Mean skew uses 15 valid observations; processing-backlog growth uses 15 valid growth values. Missing observations and ownership changes leave gaps until windows refill.'


def style(ax):
    ax.spines[['top', 'right']].set_visible(False)
    ax.grid(axis='x', color='#dfe4e8', linewidth=0.6)
    ax.set_axisbelow(True)


def save(fig, output, name):
    for extension in ['png', 'pdf', 'svg']:
        fig.savefig(output / f'{name}.{extension}', dpi=190, bbox_inches='tight',
                    facecolor='white')


def render(data, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MultipleLocator, StrMethodFormatter

    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.titlesize': 11, 'axes.labelsize': 10,
                         'pdf.fonttype': 42, 'ps.fonttype': 42})
    legend = [Line2D([], [], color=c, lw=2, label=l) for c, l in zip(COLORS, LABELS)]
    legend += [Line2D([], [], color='#333333', lw=1.5, label='Run 1'),
               Line2D([], [], color='#333333', lw=1.5, ls='--', label='Run 2')]

    def panels(metrics, title, note, name, height):
        fig, axes = plt.subplots(len(metrics), 2, figsize=(12.6, height),
                                 sharex=True, sharey='row', squeeze=False)
        fig.subplots_adjust(left=.10, right=.97, top=.82, bottom=.14,
                            hspace=.24, wspace=.18)
        fig.suptitle(title, y=.98, fontsize=15, fontweight='bold')
        fig.text(.5, .945, '600 messages/s during evaluation | 80% of input to 12 of 60 partitions | 16 trials',
                 ha='center', fontsize=10)
        fig.legend(handles=legend, loc='upper center', bbox_to_anchor=(.5, .92),
                   ncol=3, frameon=False)
        limits = {}
        for key, _ in metrics:
            values = [point[1] for row in data['rows']
                      for point in row.get(key, row.get('rolling', {}).get(key, []))
                      if point[1] is not None]
            step = {'processing_backlog': 25000, 'throughput': 500,
                    'window_p99': 50, 'growth': 250, 'skew': 2}[key]
            limits[key] = (min(0, math.floor(min(values)/step)*step) if key == 'growth' else 0,
                           max(step, math.ceil(max(values)/step)*step))
        limits['processing_backlog'] = (0, 185000)
        for col, condition in enumerate(CONDITIONS):
            axes[0, col].set_title(condition.title()+' starting backlog', pad=9)
            for n, (key, label) in enumerate(metrics):
                ax = axes[n, col]
                for row in [r for r in data['rows'] if r['condition'] == condition]:
                    points = row.get(key, row.get('rolling', {}).get(key))
                    if points is None:
                        raise ValueError('Missing time series: '+key)
                    x = [p[0]/60 for p in points]
                    y = [p[1] if p[1] is not None else float('nan') for p in points]
                    ax.plot(x, y, color=COLORS[ARMS.index(row['arm'])],
                            ls='-' if row['run'] == 1 else '--', lw=1.25)
                ax.axvline(1, ls=':', color='#202020', lw=1)
                ax.axvspan(.5, 1, color='#d8dee5', alpha=.35, zorder=0)
                ax.axvspan(9.5, 10, color='#d8dee5', alpha=.35, zorder=0)
                ax.set_xlim(0, 10)
                ax.xaxis.set_major_locator(MultipleLocator(1))
                if col == 0:
                    ax.set_ylabel(label)
                if n == len(metrics)-1:
                    ax.set_xlabel('Evaluation time (min)')
                ax.set_ylim(*limits[key])
                if key == 'growth':
                    ax.axhline(0, color='#888888', lw=.7)
                if key == 'throughput':
                    ax.axhline(600, ls=':', color='#777777', lw=.7)
                    ax.yaxis.set_major_locator(MultipleLocator(500))
                if key == 'processing_backlog':
                    ax.set_ylim(0, 185000)
                    ax.yaxis.set_major_locator(MultipleLocator(25000))
                    ax.yaxis.set_minor_locator(MultipleLocator(2500))
                ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}' if key != 'skew' else '{x:g}'))
                ax.grid(axis='y', color='#e7e9eb', linewidth=.6)
                style(ax)
            for left, right, label in [(.5, 1, 'Before'), (9.5, 10, 'Eval. end')]:
                axes[0,col].text((left+right)/2, 1.015, label,
                    transform=axes[0,col].get_xaxis_transform(), ha='center', fontsize=8)
        fig.text(.10, .025, note, fontsize=9, va='bottom', linespacing=1.4)
        save(fig, output, name)
        plt.close(fig)

    panels([('processing_backlog', 'Processing backlog (offsets)'),
            ('throughput', 'Throughput (msg/s)\n10-second intervals'),
            ('window_p99', 'Window p99 end-to-end latency (s)\n30-second completion intervals')],
           'Performance evaluation metrics: before and after mitigation',
           'The dotted vertical line marks the scheduled action at minute 1; activation occurs later and differs between actions.\n'
           'Throughput includes warm-up messages completed during evaluation. Window p99 uses evaluation messages completed in each interval.\n'
           'Points are shown at interval ends; these window percentiles are not whole-run p99. Missing observations remain gaps. Drain is excluded.',
           'starting-backlog-performance', 11.7)
    panels([('growth', 'Mean processing-backlog growth (offsets/s)\nRolling 30 seconds'),
            ('skew', 'Mean lag skew\n15-observation rolling window')],
           'Runtime control signals: before and after mitigation',
           'Windows update every 2 seconds. Mean growth requires 16 observations; mean skew uses 15 observations spanning 28 seconds.\n'
           'Ownership changes, missing data and excessive gaps reset affected windows. Unavailable values are not zero or interpolated.\n'
           'Negative growth indicates decreasing backlog. Read skew alongside backlog size: high relative skew can remain when little work is queued.',
           'starting-backlog-control-signals', 8.6)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--data', type=Path, help='Render a previously verified compact data file.')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    data = json.loads(args.data.read_text()) if args.data else collect(args.repo)
    if not args.data:
        add_completion_windows(args.repo, data, args.output / 'window-cache')
    args.output.mkdir(parents=True, exist_ok=True)
    if not args.data:
        (args.output / 'starting-backlog-plot-data.json').write_text(
            json.dumps(data, indent=2, allow_nan=False)+'\n')
    render(data, args.output)
    print(f"Rendered {len(data['rows'])} individual trials in {args.output}")


if __name__ == '__main__':
    main()
