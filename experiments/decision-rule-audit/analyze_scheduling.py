"""Describe observed partition service order without assuming a fetch scheduler."""
import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def percentile(values, fraction):
    return sorted(values)[math.ceil(fraction * len(values)) - 1] if values else None


def describe(rows, seconds=30):
    """Rows retain the consumer writer's order; replayed completions remain work."""
    streaks, backwards, offsets, producers = [], [], {}, {}
    lengths, streak = Counter(), 0
    previous, offset_errors, time_errors, producer_pairs = None, 0, 0, 0
    same_partition = 0
    for row in rows:
        key = (row['topic'], row['partition'], row['assignment_epoch'])
        if previous is not None:
            previous_key = (previous['topic'], previous['partition'], previous['assignment_epoch'])
            same_partition += key == previous_key
            if key != previous_key:
                streaks.append(streak)
                streak = 0
            time_errors += row['processing_start_timestamp'] < previous['completion_timestamp']
        streak += 1
        lengths[row['partition']] += 1
        offset_errors += key in offsets and row['offset'] <= offsets[key]
        offsets[key] = row['offset']
        # Comparing timestamps from the same producer process avoids comparing
        # creation clocks on different producer nodes. This is a subsequence,
        # not necessarily two adjacent completions in the full consumer stream.
        producer = row['message_id'].rsplit('/', 1)[0]
        earlier = producers.get(producer)
        if earlier is not None and earlier['partition'] != row['partition']:
            producer_pairs += 1
            backwards.append(max(0, earlier['producer_timestamp'] - row['producer_timestamp']))
        producers[producer] = row
        previous = row
    if streak:
        streaks.append(streak)
    waits = defaultdict(list)
    for row in rows:
        waits[row['partition']].append(row['processing_start_timestamp'] - row['producer_timestamp'])
    return {
        'selected_processing_starts': len(rows),
        'observed_start_rate': len(rows) / seconds,
        'assignment_epochs': sorted({row['assignment_epoch'] for row in rows}),
        'same_partition_adjacent_pairs': same_partition,
        'adjacent_pairs': max(0, len(rows) - 1),
        'same_partition_adjacent_fraction': same_partition / (len(rows) - 1) if len(rows) > 1 else None,
        'streak_mean': statistics.mean(streaks) if streaks else None,
        'streak_p95': percentile(streaks, .95),
        'streak_max': max(streaks, default=None),
        'streak_histogram': dict(sorted(Counter(streaks).items())),
        'same_producer_cross_partition_pairs': producer_pairs,
        'creation_time_backtracks_over_10ms': sum(x > .01 for x in backwards),
        'creation_time_backtracks_over_100ms': sum(x > .1 for x in backwards),
        'creation_time_backtracks_over_1s': sum(x > 1 for x in backwards),
        'largest_creation_time_backtrack_seconds': max(backwards, default=None),
        'nonincreasing_offsets_within_epoch': offset_errors,
        'overlapping_or_reversed_processing_times': time_errors,
        'task_time_mean_seconds': statistics.mean(row['processing_seconds'] for row in rows) if rows else None,
        'partitions': [{
            'partition': partition, 'processing_starts': count,
            'observed_start_rate': count / seconds,
            'median_creation_to_start_seconds': statistics.median(waits[partition]),
        } for partition, count in sorted(lengths.items())],
    }


def monitoring(series, evaluation_start, start=30, end=60):
    """Keep only fresh, uniquely owned observations from the selected interval."""
    names = {'consumer_processing_backlog', 'consumer_lag_valid',
             'consumer_lag_observation_age_seconds', 'consumer_partition_owned'}
    samples = defaultdict(lambda: defaultdict(dict))
    for item in series:
        labels = item['metric']
        name = labels['__name__']
        if name not in names:
            continue
        process = (labels['pod'], labels['incarnation'])
        partition = (labels['topic'], int(labels['partition']))
        for timestamp, value in item['values']:
            # query_range uses millisecond boundaries; the manifest has more digits.
            relative = round(timestamp - evaluation_start, 3)
            if start <= relative < end:
                samples[(relative, partition)][process][name] = float(value)
    result = defaultdict(lambda: defaultdict(list))
    for (_, (_, partition)), processes in samples.items():
        owners = [(process, values) for process, values in processes.items()
                  if values.get('consumer_partition_owned') == 1]
        if len(owners) != 1:
            continue
        process, values = owners[0]
        backlog = values.get('consumer_processing_backlog', math.nan)
        age = values.get('consumer_lag_observation_age_seconds', math.inf)
        if values.get('consumer_lag_valid') == 1 and 0 <= age <= 10 and math.isfinite(backlog) and backlog >= 0:
            result[process][partition].append(backlog)
    return result


def analyze_run(root, run, source_hashes):
    directory = root / 'results' / run['run_id']
    manifest = json.loads((directory / 'manifest.json').read_text())
    status = json.loads((directory / 'runner-status.json').read_text())
    if status['status'] != 'complete' or status['run_id'] != manifest['run_id']:
        raise ValueError('Run is not complete: ' + run['run_id'])
    evaluation = manifest['evaluation_start_epoch']
    observations = json.loads((directory / 'prometheus.json').read_text())
    if observations['status'] != 'success':
        raise ValueError('Monitoring export failed')
    backlog = monitoring(observations['data']['result'], evaluation)
    used = [directory / name for name in ('manifest.json', 'runner-status.json', 'prometheus.json')]
    result = {key: run[key] for key in ('run_id', 'backlog_condition', 'arm', 'run_number')}
    result.update(window_seconds=[30, 60], poll_max_messages=int(manifest['config']['POLL_MAX_MSG']),
                  assignment_mode=manifest['config']['CONSUMER_ASSIGNMENT_MODE'], processes=[],
                  hot_partitions=[int(p) for p in manifest['config']['SKEW_PARTITION'].split(',')])
    paths = sorted(list((directory / 'consumer').glob('*/*/events.jsonl')) +
                   list((directory / 'consumer').glob('*/*/events.jsonl.gz')))
    if len({path.parent for path in paths}) != len(paths):
        raise ValueError('Duplicate compressed and plain evidence')
    if not paths:
        raise ValueError('Missing completion evidence')
    for path in paths:
        rows, metadata = [], None
        opener = gzip.open if path.suffix == '.gz' else open
        with opener(path, 'rt', encoding='utf-8') as stream:
            for line in stream:
                row = json.loads(line)
                if row['event'] == 'started':
                    metadata = row
                if row['event'] != 'completed':
                    continue
                if evaluation + 30 <= row['processing_start_timestamp'] < evaluation + 60:
                    numeric = ['processing_start_timestamp', 'completion_timestamp',
                               'producer_timestamp', 'processing_seconds']
                    if not all(math.isfinite(row[name]) for name in numeric) or row['processing_seconds'] < 0:
                        raise ValueError('Invalid completion record')
                    if row['completion_timestamp'] < row['processing_start_timestamp']:
                        raise ValueError('Reversed processing timestamps')
                    rows.append(row)
        if not rows:
            continue
        final_path = path.parent / 'final.json'
        final = json.loads(final_path.read_text())
        if final.get('evidence_dropped') != 0 or final.get('evidence_error') or final.get('failure'):
            raise ValueError('Consumer evidence is incomplete: ' + str(path))
        if metadata is None:
            raise ValueError('Missing process metadata')
        pod, incarnation = path.parts[-3:-1]
        summary = describe(rows)
        summary.update(pod=pod, incarnation=incarnation, packages=metadata['packages'],
                       recorded_consumer_sha256=metadata['source_sha256']['consumer.py'],
                       matches_reviewed_consumer=metadata['source_sha256']['consumer.py'] == source_hashes['consumer.py'])
        for partition in summary['partitions']:
            values = backlog[(pod, incarnation)][partition['partition']]
            partition.update(valid_backlog_samples=len(values), expected_backlog_samples=15,
                             positive_backlog_samples=sum(x > 0 for x in values),
                             minimum_observed_backlog=min(values, default=None))
        # A small plotted excerpt is enough; large raw logs stay outside Git.
        summary['first_200_processing_starts'] = [
            {'relative_start': row['processing_start_timestamp'] - evaluation,
             'partition': row['partition']} for row in rows[:200]]
        result['processes'].append(summary)
        used.extend([path, final_path])
    if not result['processes']:
        raise ValueError('No processing in reporting window')
    return result, [{'path': str(path.relative_to(root)), 'sha256': digest(path)} for path in used]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    progress_path = args.repo / 'experiment-records/backlog-comparison-20261027/progress.json'
    progress = json.loads(progress_path.read_text())
    sources = {'consumer.py': digest(args.repo / 'src/consumer/consumer.py')}
    results, evidence = [], []
    for run in progress['runs']:
        result, paths = analyze_run(args.repo, run, sources)
        results.append(result)
        evidence.extend(paths)
        print(run['run_id'] + ': ' + str(len(result['processes'])) + ' pre-action consumer streams', flush=True)
    report = {
        'scope': 'Saved application processing order in evaluation seconds [30,60), before interventions, across the 16 backlog-comparison trials. No new trials.',
        'selection': 'Physical completions whose processing began in the interval. Repeated processing is retained; observed start rates are not distinct useful throughput or capacity.',
        'streaks': 'Consecutive records from one topic-partition and assignment epoch in writer order. First and last streaks may be cut by the reporting window. These are not fetch batch sizes.',
        'timestamp_comparisons': 'Cross-partition consecutive members of each producer-process subsequence. Creation-time backtracks compare one producer clock; processing waits across machines retain clock uncertainty.',
        'monitoring': 'Fifteen 2-second samples at evaluation seconds 30 through 58, rounded to milliseconds. Require one observed owner, valid=1, age<=10 s, and finite nonnegative processing backlog. Positive samples do not prove uninterrupted local-buffer availability.',
        'limitations': 'No consume-call boundaries, per-record fetch times, or local-buffer head snapshots were logged. These records do not identify an exact scheduling algorithm or validate a counterfactual scheduling change.',
        'reviewed_source_sha256': sources, 'runs': results,
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'results.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    evidence.append({'path': str(progress_path.relative_to(args.repo)), 'sha256': digest(progress_path)})
    (args.output / 'source-manifest.json').write_text(json.dumps(evidence, indent=2) + '\n')
    fields = ['run_id', 'backlog_condition', 'arm', 'run_number', 'pod', 'selected_processing_starts',
              'observed_start_rate', 'same_partition_adjacent_fraction', 'streak_mean', 'streak_p95',
              'streak_max', 'creation_time_backtracks_over_10ms', 'creation_time_backtracks_over_100ms',
              'creation_time_backtracks_over_1s', 'largest_creation_time_backtrack_seconds',
              'nonincreasing_offsets_within_epoch', 'overlapping_or_reversed_processing_times']
    with (args.output / 'consumer-summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fields, lineterminator='\n', extrasaction='ignore')
        writer.writeheader()
        for run in results:
            for process in run['processes']:
                writer.writerow(dict(run, **process))


if __name__ == '__main__':
    main()
