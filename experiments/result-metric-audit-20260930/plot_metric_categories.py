"""Reproduce the proposal's performance, control-signal and resource traces.

Each curve is one recorded trial. The two deployment rates and two backlog
conditions remain separate, and missing monitoring observations remain gaps.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/stream-category-plot-cache')

ARMS = ['keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6']
LABELS = ['Keep 3', 'Redistribute 3', 'Targeted scale 6', 'Kafka scale 6']
COLORS = ['#2563a6', '#dc862d', '#21875e', '#9a4eaa']
CAMPAIGNS = {
    'twelve700': 'experiment-records/four-condition-20260929/comparison.json',
    'four700': 'experiment-records/four-hot-partitions-20260930/comparison.json',
    'four600': 'results/four-hot600-20261021-analysis/comparison.json',
    'starting': 'results/four-condition-20261027-003002-analysis/comparison.json',
}


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(path.read_text())


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def monitoring_series(lag, start, end):
    names = {
        'processing_backlog': 'processing_backlog',
        'growth': 'window_processing_backlog_growth_offsets_per_second',
        'skew': 'window_mean_skew',
        'persistent_hot': 'persistent_hot',
    }
    series = {key: [] for key in names}
    previous = None
    assert lag['parameters']['window_samples'] == 15
    assert lag['parameters'].get('growth_window_samples', 15) == 15
    for sample in lag['snapshots']:
        t = sample['timestamp'] - start
        if not 0 <= t <= end - start:
            continue
        discontinuity = previous is not None and (
            sample['timestamp'] - previous['timestamp'] > 3
            or sample.get('owners') != previous.get('owners'))
        for name, source in names.items():
            if discontinuity:
                series[name].append([t, None])
            value = sample.get(source) if sample['valid'] else None
            if name == 'persistent_hot' and value is not None:
                value = len(value)
            series[name].append([t, value])
        previous = sample
    return series


def collect(repo, cache, previous_resources=None, campaigns=None):
    sys.path.insert(0, str(repo / 'python-scripts'))
    sys.path.insert(0, str(repo / 'experiments/result-metric-audit-20260930'))
    from evidence_io import event_paths, open_events
    from build_compact_figures import process_total
    windows = load_module('category_windows', repo / 'experiments/four-condition-20260929/analyze_action_windows.py')
    cache.mkdir(parents=True, exist_ok=True)
    rows, comparison_hashes = [], {}
    saved_windows = {}
    for campaign, relative in [
            ('twelve700', 'experiment-records/four-condition-20260929/action-window-performance.json'),
            ('four600', 'results/four-hot600-20261021-analysis/action-window-performance.json')]:
        path = repo / relative
        saved_windows[campaign] = {r['run_id']: (r, relative, sha(path)) for r in read(path)['runs']}
    starting_path = repo / 'experiment-records/backlog-comparison-20261027/starting-backlog-plot-data.json'
    starting = {r['run_id']: r for r in read(starting_path)['rows']}
    previous = read(previous_resources) if previous_resources else {}
    resource_path = repo / 'results/four-hot600-20261021-analysis/figures/four-hot600-resource-series.json'
    four600_resources = read(resource_path)
    for campaign, relative in CAMPAIGNS.items():
        if campaigns and campaign not in campaigns:
            continue
        path = repo / relative
        comparison = read(path)
        comparison_hashes[relative] = sha(path)
        expected_count = {'twelve700': 8, 'four700': 4, 'four600': 8, 'starting': 16}[campaign]
        assert len(comparison['runs']) == expected_count
        for source in comparison['runs']:
            directory = repo / source['raw_evidence']
            cached = cache / (source['run_id'] + '.json')
            signature = hashlib.sha256(json.dumps(source, sort_keys=True).encode()).hexdigest()
            if cached.exists():
                row = read(cached)
                assert row['comparison_row_sha256'] == signature
                rows.append(row)
                print('Cached:', campaign, source['arm'], source['run_number'], flush=True)
                continue
            print('Reading:', campaign, source['arm'], source['run_number'], flush=True)
            hashes = {}
            for name in ('manifest.json', 'lag-summary.json', 'prometheus.json'):
                hashes[name] = sha(directory / name)
                assert hashes[name] == source['evidence_hashes'][name], (source['run_id'], name)
            manifest = read(directory / 'manifest.json')
            start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
            assert math.isclose(end - start, 600)
            row = dict(run_id=source['run_id'], campaign=campaign,
                       arm=source['arm'], run=source['run_number'],
                       condition=source.get('backlog_condition'),
                       rate=700 if campaign.endswith('700') else 600,
                       source_hashes=hashes, comparison_row_sha256=signature,
                       whole_run_p99_seconds=source['p99_seconds'],
                       unfinished_percent=source['unfinished_percent'])
            row.update(monitoring_series(read(directory / 'lag-summary.json'), start, end))
            if campaign in saved_windows:
                saved, relative, digest = saved_windows[campaign][source['run_id']]
                assert saved['event_file_sha256'] == source['intervention_cost']['event_file_sha256']
                assert saved['whole_run_replay_matches'] is True
                assert saved['whole_run_unfinished'] == source['unfinished']
                selected = [w for w in saved['rolling'] if w['end_after_evaluation_seconds'] % 30 == 0]
                assert len(selected) == 20
                row['throughput'] = [[w['end_after_evaluation_seconds'], w['completion_throughput_per_second']] for w in selected]
                row['window_p99'] = [[w['end_after_evaluation_seconds'], w['completion_window_p99_seconds']] for w in selected]
                row['completion_summary'] = {'path': relative, 'sha256': digest}
                event_hashes = saved['event_file_sha256']
            elif campaign == 'starting':
                saved = starting[source['run_id']]
                assert saved['message_evidence_sha256'] == source['intervention_cost']['event_file_sha256']
                assert saved['manifest_sha256'] == hashes['manifest.json']
                assert saved['completed'] == source['completed']
                assert saved['p99_seconds'] == source['p99_seconds']
                row['throughput'] = [[saved['throughput'][i+2][0], sum(p[1] for p in saved['throughput'][i:i+3])/3]
                                     for i in range(0, 60, 3)]
                row['window_p99'] = saved['window_p99']
                row['completion_summary'] = {'path': str(starting_path.relative_to(repo)), 'sha256': sha(starting_path)}
                event_hashes = saved['message_evidence_sha256']
            else:
                ack, done, event_hashes = windows.load_messages(
                    directory, manifest, source['intervention_cost']['event_file_sha256'])
                latencies = [value[0] - ack[key][0] for key, value in done.items()
                             if start <= ack[key][0] < end
                             and value[0] <= manifest['drain_end_epoch']]
                assert len(latencies) == source['completed']
                assert math.isclose(windows.p99(latencies), source['p99_seconds'], abs_tol=1e-9)
                assert sum(start <= v[0] < end for v in ack.values()) == source['admitted']
                series = windows.build_series(ack, done, start, end)
                row['throughput'] = [[t + 30, windows.window_metrics(series, start+t, start+t+30)['completion_throughput_per_second']]
                                     for t in range(0, 600, 30)]
                row['window_p99'] = [[t + 30, windows.window_metrics(series, start+t, start+t+30)['completion_window_p99_seconds']]
                                     for t in range(0, 600, 30)]
                del ack, done, latencies, series
            assert math.isclose(sum(p[1]*30 for p in row['throughput']) / 600,
                                source['useful_throughput'], rel_tol=1e-10)
            row['event_sha256'] = event_hashes
            starts = {}
            if source['run_id'] in previous:
                saved = previous[source['run_id']]
                assert saved['event_sha256'] == event_hashes
                starts = saved['consumer_start_epoch']
                row['start_summary_sha256'] = sha(previous_resources)
            for event_path in ([] if starts or campaign == 'four600' else event_paths(directory)):
                final = read(event_path.parent / 'final.json')
                if final['role'] != 'consumer':
                    continue
                with open_events(event_path) as stream:
                    first = json.loads(next(stream))
                assert first['event'] == 'started'
                assert final['pod'] not in starts
                starts[final['pod']] = first['timestamp']
            prom = read(directory / 'prometheus.json')['data']['result']
            for key, metric, divisor in [('cpu', 'consumer_cpu_percent', 100),
                                         ('memory', 'consumer_memory_bytes', 2**20)]:
                if campaign == 'four600':
                    observed = four600_resources[source['run_id']][metric]
                    row['resource_summary'] = {'path': str(resource_path.relative_to(repo)), 'sha256': sha(resource_path)}
                else:
                    observed = process_total(prom, metric, divisor, starts, start, end)
                row[key] = [[t * 60, v] for t, v in zip(observed['minutes'], observed['values'])]
            cached.write_text(json.dumps(row, allow_nan=False) + '\n')
            rows.append(row)
            print('Verified:', source['run_id'], flush=True)
    expected = sum({'twelve700': 8, 'four700': 4, 'four600': 8, 'starting': 16}[c]
                   for c in (campaigns or CAMPAIGNS))
    assert len(rows) == len({r['run_id'] for r in rows}) == expected
    return dict(rows=rows, comparison_sha256=comparison_hashes,
                definitions={
                    'processing_backlog': 'Valid Q(t) observations; ownership changes and missing observations break the line.',
                    'throughput': 'Distinct first completions per nonoverlapping 30-second interval; includes warm-up messages completed during evaluation.',
                    'window_p99': 'Nearest-rank p99 of creation-to-completion latency for evaluation messages completed in each nonoverlapping 30-second interval.',
                    'growth': 'Mean processing-backlog growth over 15 valid two-second intervals.',
                    'skew': 'Mean lag skew over 15 consecutive valid observations.',
                    'persistent_hot': 'Number of persistently hot partitions from the existing lag-based calculation.',
                    'resources': 'CPU and RSS totals only where all started consumer processes have fresh observations; no missing component is replaced with zero.',
                    'scope': 'Evaluation only, excluding warm-up and drain. No averaging across trials or deployments.'})


def attach_activation_boundaries(data, repository):
    """Verify each marker against the original handover or ownership record."""
    windows = load_module('activation_windows', repository / 'experiments/four-condition-20260929/analyze_action_windows.py')
    comparisons = {}
    for campaign, relative in CAMPAIGNS.items():
        path = repository / relative
        assert sha(path) == data['comparison_sha256'][relative], relative
        comparisons[campaign] = {r['run_id']: r for r in read(path)['runs']}
    for row in data['rows']:
        source = comparisons[row['campaign']][row['run_id']]
        assert source['arm'] == row['arm'] and source['run_number'] == row['run']
        assert source['intervention_cost']['event_file_sha256'] == row['event_sha256']
        directory = repository / source['raw_evidence']
        manifest_path = directory / 'manifest.json'
        assert sha(manifest_path) == row['source_hashes']['manifest.json']
        manifest = read(manifest_path)
        assert manifest['run_id'] == row['run_id']
        assert math.isclose(manifest['producer_end_epoch'] - manifest['evaluation_start_epoch'], 600)
        if row['arm'] == 'kafka_scale6':
            # Native confirmation depends on the saved sequence of ownership observations.
            assert sha(directory / 'lag-summary.json') == row['source_hashes']['lag-summary.json']
        boundary = windows.post_action_boundary(directory, manifest, source)
        if source.get('activation'):
            assert boundary == source['activation']
        row['activation'] = boundary
        print('Verified activation:', row['run_id'], row['arm'],
              round(boundary['after_evaluation_seconds'], 3), flush=True)


def render(data, out, allow_partial=False):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MaxNLocator, MultipleLocator, StrMethodFormatter
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11,
                         'axes.labelsize': 11, 'axes.titlesize': 12,
                         'pdf.fonttype': 42, 'ps.fonttype': 42})
    out.mkdir(parents=True, exist_ok=True)
    groups = [
        ('results-12hot-700-runtime', '12 high-input partitions | 700 messages/s | 8 trials',
         [('Run 1', lambda r: r['campaign'] == 'twelve700' and r['run'] == 1),
          ('Run 2', lambda r: r['campaign'] == 'twelve700' and r['run'] == 2)], False),
        ('results-4hot-runtime', '4 high-input partitions | deployment blocks shown separately | 12 trials',
         [('700 messages/s', lambda r: r['campaign'] == 'four700'),
          ('600 messages/s', lambda r: r['campaign'] == 'four600')], True),
        ('results-starting-backlog-runtime', '12 high-input partitions | 600 messages/s during evaluation | 16 trials',
         [('Smaller starting backlog', lambda r: r['campaign'] == 'starting' and r['condition'] == 'smaller'),
          ('Larger starting backlog', lambda r: r['campaign'] == 'starting' and r['condition'] == 'larger')], True),
    ]
    categories = [
        ('performance', 'Performance Evaluation Metrics',
         [('processing_backlog', 'Processing backlog\n(offsets)'), ('throughput', 'Throughput (msg/s)'),
          ('window_p99', 'Window p99\nend-to-end latency (s)')],
         'Throughput: 30-second intervals. P99: evaluation messages completed within each 30-second interval.\n'
         'Window p99 excludes drain and differs from whole-run p99; unfinished messages have no completed latency.'),
        ('control-signals', 'Runtime Control Signals',
         [('growth', 'Mean processing-backlog\ngrowth (offsets/s)'), ('skew', 'Mean lag skew ratio'),
          ('persistent_hot', 'Persistently hot\npartitions (count)')],
         'Processing-backlog growth: 15 valid two-second intervals. Lag skew and persistence: 15 consecutive valid observations.\n'
         'Missing data and ownership changes leave gaps until affected windows refill.'),
        ('costs', 'Mitigation Cost Metrics',
         [('cpu', 'Actual CPU use\n(cores)'), ('memory', 'Actual RSS memory\n(MiB)')],
         'Observed use across all started consumer processes; missing or stale components leave a gap.\n'
         'These traces show actual use; requested resources and intervention delays are separate measures.'),
    ]
    audit = []
    figure_count = 0
    for name, subtitle, panels, styles in groups:
        selected = [[r for r in data['rows'] if predicate(r)] for _, predicate in panels]
        if not all(selected):
            assert allow_partial, 'Missing a comparison panel: ' + name
            continue
        all_rows = selected[0] + selected[1]
        legend = [Line2D([], [], color=color, lw=2, label=label)
                  for color, label in zip(COLORS, LABELS)]
        if styles:
            legend += [Line2D([], [], color='#333333', lw=1.5, label='Run 1'),
                       Line2D([], [], color='#333333', lw=1.5, ls='--', label='Run 2')]
        for suffix, title, metrics, note in categories:
            height = 10.5 if len(metrics) == 3 else 8.3
            compact = suffix == 'performance' and name in (
                'results-12hot-700-runtime', 'results-4hot-runtime')
            if compact:
                # Keep text size while fitting these panels beside their discussion.
                height = 6.1 if name == 'results-12hot-700-runtime' else 6.7
            fig, axes = plt.subplots(len(metrics), 2, figsize=(9.5, height),
                                     sharex=True, sharey='row', squeeze=False)
            fig.subplots_adjust(left=.14, right=.98,
                                top=(.80 if not styles else .78) if compact else .82,
                                bottom=(.19 if not styles else .18) if compact else .14,
                                wspace=.18, hspace=.23)
            fig.suptitle(title, y=.985, fontsize=14, fontweight='bold')
            fig.text(.5, .925 if compact else .95, subtitle, ha='center', fontsize=10)
            fig.legend(handles=legend, loc='upper center',
                       bbox_to_anchor=(.5, .89 if compact else .925),
                       ncol=3 if styles else 4, frameon=False, fontsize=9)
            for i, (key, label) in enumerate(metrics):
                if compact:
                    label = {'processing_backlog': 'Processing\nbacklog (offsets)',
                             'throughput': 'Throughput\n(msg/s)',
                             'window_p99': 'Window p99\nend-to-end\nlatency (s)'}.get(key, label)
                values = [v for row in all_rows for _, v in row[key] if v is not None]
                assert values and all(math.isfinite(v) for v in values), (name, key)
                low, high = min(0, min(values)), max(values)
                if low < 0:
                    low *= 1.08
                high = max(high * 1.08, 1)
                for col, (panel_name, _) in enumerate(panels):
                    ax = axes[i, col]
                    for row in selected[col]:
                        points = row[key]
                        ax.plot([t/60 for t, _ in points],
                                [math.nan if v is None else v for _, v in points],
                                color=COLORS[ARMS.index(row['arm'])],
                                ls='--' if styles and row['run'] == 2 else '-', lw=1.2)
                    ax.axvline(1, color='#666666', ls=':', lw=1, zorder=1)
                    for row in selected[col]:
                        boundary = row['activation']
                        seconds = boundary['after_evaluation_seconds']
                        assert 60 <= seconds < 600
                        if row['arm'] == 'keep3':
                            assert seconds == 60 and boundary['basis'] == 'scheduled reference; no action'
                            continue
                        ax.axvline(seconds / 60, color=COLORS[ARMS.index(row['arm'])],
                                   ls=':', lw=1.25, alpha=.85, zorder=1)

                    ax.axvspan(.5, 1, color='#d8dee5', alpha=.35, zorder=0)
                    ax.axvspan(9.5, 10, color='#d8dee5', alpha=.35, zorder=0)
                    ax.set_xlim(0, 10); ax.set_ylim(low, high)
                    ax.xaxis.set_major_locator(MultipleLocator(1))
                    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, integer=key == 'persistent_hot'))
                    if key == 'throughput':
                        ax.axhline(selected[col][0]['rate'], color='#777777', ls=':', lw=.7)
                        ax.yaxis.set_major_locator(MultipleLocator(500))
                    if key == 'processing_backlog':
                        ax.yaxis.set_minor_locator(MultipleLocator(2500))
                    if key == 'growth':
                        ax.axhline(0, color='#777777', lw=.6)
                    ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}' if key not in ('skew', 'cpu') else '{x:g}'))
                    ax.spines[['top', 'right']].set_visible(False)
                    ax.grid(alpha=.2); ax.set_axisbelow(True)
                    if col == 0:
                        ax.set_ylabel(label)
                    if i == 0:
                        ax.set_title(panel_name, pad=8)
                        ax.text(.75, 1.03, 'Before', transform=ax.get_xaxis_transform(), ha='center', fontsize=8, color='#666666')
                        ax.text(9.75, 1.03, 'Eval. end', transform=ax.get_xaxis_transform(), ha='center', fontsize=8, color='#666666')
                    if i == len(metrics)-1:
                        ax.set_xlabel('Evaluation time (min)')
                audit.append(dict(figure=name+'-'+suffix, metric=key,
                                  valid_min=min(values), valid_max=max(values),
                                  axis_min=low, axis_max=high))
            fig.text(.545, .035, note+'\nDotted vertical lines: gray = request; colors = confirmed activation for each run. Shading: Before / Eval. end.',
                     ha='center', fontsize=8)
            for extension in ('pdf', 'png', 'svg'):
                path = out/(name+'-'+suffix+'.'+extension)
                fig.savefig(path, dpi=190, bbox_inches='tight', facecolor='white')
                if extension == 'svg':
                    path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines()) + '\n')
            plt.close(fig)
            figure_count += 1
    (out/'activation-validation.json').write_text(json.dumps({
        'note': 'Markers use recorded confirmation, not action request, recovery or a rounded window. Keep 3 has no activation marker.',
        'comparison_sha256': data['comparison_sha256'],
        'runs': [dict(run_id=r['run_id'], campaign=r['campaign'], arm=r['arm'],
                      run=r['run'], condition=r['condition'],
                      marker_drawn=r['arm'] != 'keep3', **r['activation']) for r in data['rows']]
    }, indent=2) + '\n')
    (out/'plot-validation.json').write_text(json.dumps(audit, indent=2)+'\n')
    return figure_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path)
    parser.add_argument('--input', type=Path, help='Replot the saved, verified compact data without raw evidence.')
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--previous-resources', type=Path, help='Existing verified plot data containing original consumer start times.')
    parser.add_argument('--campaign', nargs='+', choices=list(CAMPAIGNS), help='Collect selected campaign blocks.')
    parser.add_argument('--collect-only', action='store_true', help='Save verified data without rendering figures.')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.input:
        data = read(args.input)
    else:
        if not args.repository or not args.cache:
            parser.error('--repository and --cache are required when collecting evidence')
        data = collect(args.repository, args.cache, args.previous_resources, args.campaign)
    args.output.mkdir(parents=True, exist_ok=True)
    if not args.collect_only and not args.campaign:
        assert len(data['rows']) == len({r['run_id'] for r in data['rows']}) == 36
    if args.repository:
        attach_activation_boundaries(data, args.repository)
    elif any('activation' not in row for row in data['rows']):
        parser.error('--repository is required to verify activation boundaries for older plot data')
    data['plotter_sha256'] = sha(Path(__file__))
    (args.output/'metric-category-plot-data.json').write_text(json.dumps(data, allow_nan=False)+'\n')
    if args.collect_only:
        print('Saved verified plot data for', len(data['rows']), 'trials.', flush=True)
    else:
        count = render(data, args.output, allow_partial=bool(args.campaign))
        print('Saved', count, 'category figures for', len(data['rows']), 'skewed-workload trials.', flush=True)


if __name__ == '__main__':
    main()
