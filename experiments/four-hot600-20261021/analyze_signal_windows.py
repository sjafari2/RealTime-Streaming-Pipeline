"""Report backlog and resource observations in the completion-window intervals."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'python-scripts'))
from analyze_lag import growth_plot_lag
from analyze_execution import resource_integral


def describe(values):
    values = [value for value in values if value is not None and math.isfinite(value)]
    return dict(samples=len(values), mean=statistics.mean(values) if values else None,
                minimum=min(values) if values else None, maximum=max(values) if values else None)


def signal_window(snapshots, left, right):
    if not math.isfinite(left + right) or right <= left:
        raise ValueError('Window must have positive finite duration')
    rows = [row for row in snapshots if left <= row['timestamp'] < right]
    valid = [row for row in rows if row.get('valid')]
    fields = ('total_lag', 'processing_backlog', 'mean_partition_lag', 'max_partition_lag',
              'skew', 'population_stddev', 'window_mean_backlog', 'window_mean_skew',
              'window_growth_offsets_per_second', 'window_processing_backlog_growth_offsets_per_second')
    return dict(observations=len(rows), valid_observations=len(valid),
                valid_observation_fraction=len(valid)/len(rows) if rows else None,
                fields={key: describe([row.get(key) for row in valid]) for key in fields},
                selected_timestamps=[row['timestamp'] for row in valid])


def process_window(series, role, pods, left, right):
    """Sum only complete, fresh per-pod observations; keep missing totals absent."""
    grid = {float(t) for item in series for t, _ in item['values'] if left <= float(t) < right}
    result = {}
    for suffix, divisor, unit in [('cpu_percent', 100, 'cores'), ('memory_bytes', 1024**2, 'MiB RSS')]:
        name = role + '_' + suffix
        per_pod, values = {}, {}
        for pod in pods:
            selected = [item for item in series if item['metric'].get('__name__') == name
                        and item['metric'].get('pod') == pod]
            stamps = [item for item in series if item['metric'].get('__name__') == name+'_scrape_timestamp_seconds'
                      and item['metric'].get('pod') == pod]
            if len(selected) > 1 or len(stamps) > 1:
                raise ValueError('Ambiguous process incarnation for ' + pod)
            fresh = {float(t): float(v) for t, v in stamps[0]['values']} if stamps else {}
            samples = {}
            for t, value in selected[0]['values'] if selected else []:
                t, value = float(t), float(value)
                if (left <= t < right and math.isfinite(value) and value >= 0
                        and t in fresh and 0 <= t-fresh[t] <= 10):
                    samples[t] = value/divisor
            values[pod] = samples
            per_pod[pod] = describe(list(samples.values()))
        times = sorted(set.intersection(*(set(values[p]) for p in pods))) if pods else []
        totals = [sum(values[p][t] for p in pods) for t in times]
        result[suffix] = dict(unit=unit, total=describe(totals), per_pod=per_pod,
                             complete_timestamps=times, expected_pods=pods, export_grid_samples=len(grid),
                             fraction_of_export_grid=len(times)/len(grid) if grid else None)
    return result


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def analyze(comparison, windows, repository, campaign=None):
    data = json.loads(comparison.read_text())
    timing = json.loads(windows.read_text())
    if timing['comparison_sha256'] != sha(comparison):
        raise ValueError('Completion windows refer to a different comparison')
    indexed = {row['run_id']: row for row in timing['runs']}
    resources = []
    if campaign is not None:
        for name, expected in data['resource_observer_sha256'].items():
            path = campaign/name
            if sha(path) != expected:
                raise ValueError('Resource observer evidence changed: '+name)
            resources.extend(json.loads(line) for line in path.read_text().splitlines())
        resources.sort(key=lambda row: row['timestamp'])
    output = []
    for row in data['runs']:
        path = repository / row['raw_evidence']
        for filename in ('manifest.json', 'lag-summary.json', 'prometheus.json'):
            if sha(path/filename) != row['evidence_hashes'][filename]:
                raise ValueError('Evidence changed: ' + str(path/filename))
        manifest = json.loads((path/'manifest.json').read_text())
        lag = json.loads((path/'lag-summary.json').read_text())
        lag = growth_plot_lag(lag, 5)
        prom = json.loads((path/'prometheus.json').read_text())['data']['result']
        summaries = {}
        for label, window in indexed[row['run_id']]['windows'].items():
            left, right = window['start_epoch'], window['end_epoch']
            count = 3 if label == 'before' or row['arm'] in ('keep3', 'redistribute3') else 6
            summaries[label] = dict(start_epoch=left, end_epoch=right,
                signals=signal_window(lag['snapshots'], left, right),
                consumer=process_window(prom, 'consumer', [f'consumer-sts-{i}' for i in range(count)], left, right),
                producer=process_window(prom, 'producer', manifest['producer_pods'], left, right),
                requested_consumer_resources=resource_integral(resources, left, right, 15) if resources else None)
        output.append(dict(run_id=row['run_id'], arm=row['arm'], run_number=row['run_number'], windows=summaries))
    return dict(schema_version=1, comparison_sha256=sha(comparison), completion_windows_sha256=sha(windows),
        definition='Arithmetic means of valid sampled values in the same half-open intervals as completion-window metrics. Process CPU is in cores, memory is process RSS. These are not requested resources or full-container memory.',
        missing_data='An aggregate process observation requires fresh values for every expected pod. Missing values remain absent. Valid sample counts, timestamps and completeness relative to the export grid are retained; a missing entire export interval remains a separate coverage limitation. No zero fill or interpolation.',
        growth='Trailing ten-second growth uses five contiguous two-second intervals with unchanged ownership; undefined windows remain absent. Fifteen-observation backlog/skew means are retained separately from raw signal averages.',
        runs=output)


def write_report(data, completion, output):
    names = dict(keep3='Keep 3', redistribute3='Redistribute 3',
                 scale_redistribute6='Targeted Scale 6', kafka_scale6='Kafka Scale 6')
    labels = dict(before='Before', post_action_to_evaluation_end='Activation to end', evaluation_end='Final 30 s')
    index = {row['run_id']: row for row in completion['runs']}
    def number(value):
        return 'Unavailable' if value is None else f'{value:,.3f}'
    def table(headers, rows):
        return ['| '+' | '.join(headers)+' |', '| '+' | '.join(['---']*len(headers))+' |',
                *['| '+' | '.join(str(v) for v in row)+' |' for row in rows], '']
    performance, signals, actual, requested = [], [], [], []
    for row in data['runs']:
        for label in labels:
            window = row['windows'][label]
            measured = index[row['run_id']]['windows'][label]
            prefix = [names[row['arm']], row['run_number'], labels[label]]
            performance.append(prefix+[number(measured.get(key)) for key in
                ('completion_throughput_per_second', 'completion_window_min_seconds',
                 'completion_window_mean_seconds', 'completion_window_max_seconds',
                 'completion_window_p99_seconds', 'acknowledged_evaluation_messages_pending_at_end')]
                +[number(100*measured['completed_window_deadline_miss_fraction']) if measured.get('completed_window_deadline_miss_fraction') is not None else 'Unavailable'])
            fields = window['signals']['fields']
            signals.append(prefix+[number(fields[key]['mean']) for key in
                ('total_lag', 'processing_backlog', 'skew', 'window_processing_backlog_growth_offsets_per_second')]
                +[f"{window['signals']['valid_observations']}/{window['signals']['observations']}"])
            actual.append(prefix+[number(window[role][field]['total']['mean'])
                for role in ('consumer', 'producer') for field in ('cpu_percent', 'memory_bytes')])
            cost = window['requested_consumer_resources']
            if cost:
                requested.append(prefix+[number(cost[key]/60 if cost[key] is not None else None) for key in
                    ('consumer_container_requested_cpu_seconds_observed', 'consumer_container_requested_gib_seconds_observed')]
                    +[number(cost['covered_fraction'])])
    lines = ['# Measurements around the scheduled responses', '', completion['windows'], '',
             'Before and final-window means use valid sampled observations. The activation-to-end interval includes backlog recovery and excludes the preceding transition. Keep 3 uses second 60 as its reference. These intervals exclude drain.', '',
             '## Completion performance', '', completion['definition'], '']
    lines += table(['Response', 'Run', 'Window', 'Throughput (msg/s)', 'Min (s)', 'Mean (s)', 'Max (s)', 'p99 (s)', 'Pending at end', 'Completed-window misses (%)'], performance)
    lines += ['## Backlog and workload signals', '', data['definition'], '',
              'B is fetched-offset lag; Q is processing backlog. Growth below is the mean of defined trailing ten-second Q-growth observations. All signal means are arithmetic; validity gaps remain missing.', '']
    lines += table(['Response', 'Run', 'Window', 'Mean B (offsets)', 'Mean Q (offsets)', 'Mean skew', 'Mean Q growth (offsets/s)', 'Valid samples'], signals)
    lines += ['## Actual process resources', '', 'CPU is measured in cores and memory is process RSS in MiB. Complete per-pod counts and coverage are in the JSON record.', '']
    lines += table(['Response', 'Run', 'Window', 'Consumer CPU', 'Consumer RSS', 'Producer CPU', 'Producer RSS'], actual)
    if requested:
        lines += ['## Requested consumer resources', '', 'These are time integrals over each stated interval. The long activation-to-end interval has a different duration from the two thirty-second windows. They are distinct from actual process usage.', '']
        lines += table(['Response', 'Run', 'Window', 'CPU (core-min)', 'Memory (GiB-min)', 'Coverage fraction'], requested)
    lines += [completion['limitations'], '', data['missing_data'], '']
    output.write_text('\n'.join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison', type=Path)
    parser.add_argument('completion_windows', type=Path)
    parser.add_argument('--repository', type=Path, default=ROOT)
    parser.add_argument('--campaign', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    data = analyze(args.comparison, args.completion_windows, args.repository, args.campaign)
    args.output.write_text(json.dumps(data, indent=2, allow_nan=False)+'\n')
    write_report(data, json.loads(args.completion_windows.read_text()), args.output.with_name('BEFORE_AFTER.md'))


if __name__ == '__main__':
    main()
