#!/usr/bin/env python3
"""Reconcile admitted messages and completion outcomes after the bounded drain."""
import argparse
import json
import math
from pathlib import Path
import sqlite3
import tempfile
from partition_outcomes import partition_metrics, quantiles


def evaluate(directory, latency_sink=None, partition_latency_sink=None):
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    start, end, drain = manifest['evaluation_start_epoch'], manifest['producer_end_epoch'], manifest['drain_end_epoch']
    deadline = float(manifest['config'].get('SLO_THRESHOLD_MS', 99)) / 1000
    invalid = []
    finals = [json.loads(p.read_text()) for p in directory.rglob('final.json')]
    observed = {role: set() for role in ('producer', 'consumer')}
    for row in finals:
        if row.get('run_id') != manifest['run_id']:
            invalid.append('Evidence from a different run')
        observed[row['role']].add(row['pod'])
        if row.get('failure') or row.get('evidence_dropped') or row.get('evidence_error'):
            invalid.append('Incomplete/failed process: ' + row['pod'] + '/' + row['incarnation'])
    for role in observed:
        if not set(manifest[role + '_pods']).issubset(observed[role]):
            invalid.append('Missing final snapshots for initial ' + role + ' pods')
    # Every incarnation that wrote events must have closed and saved its final status.
    event_files = list(directory.rglob('events.jsonl'))
    for path in event_files:
        if not path.with_name('final.json').exists():
            invalid.append('Missing final status: ' + str(path.relative_to(directory)))
    if not event_files:
        invalid.append('No outcome files')
    with tempfile.TemporaryDirectory() as temporary:
        db = sqlite3.connect(str(Path(temporary) / 'outcomes.sqlite'))
        db.executescript('''
        CREATE TABLE admitted (id TEXT PRIMARY KEY, produced REAL);
        CREATE TABLE completed (id TEXT PRIMARY KEY, finished REAL, attempts INTEGER, output TEXT, started REAL, processing REAL);
        CREATE TABLE window_completed (id TEXT PRIMARY KEY);
        ''')
        failed_sends = unresolved_sends = bad_clock = output_mismatch = 0
        completed_attempts_window = valid_attempts_window = late_attempts_window = 0
        for path in event_files:
            with path.open() as stream:
                for line in stream:
                    event = json.loads(line)
                    kind = event['event']
                    if kind == 'started' and not event.get('outcomes_enabled'):
                        invalid.append('Outcome logging disabled')
                    elif kind == 'acknowledged' and start <= event['producer_timestamp'] < end:
                        db.execute('INSERT OR IGNORE INTO admitted VALUES (?,?)', (event['message_id'], event['producer_timestamp']))
                    elif kind == 'completed':
                        timestamp = event['completion_timestamp']
                        if start <= timestamp < end:
                            completed_attempts_window += 1
                            produced = event.get('producer_timestamp')
                            if isinstance(produced, (int,float)) and math.isfinite(timestamp-produced) and timestamp >= produced:
                                valid_attempts_window += 1
                                late_attempts_window += timestamp-produced > deadline
                            db.execute('INSERT OR IGNORE INTO window_completed VALUES (?)', (event['message_id'],))
                        if timestamp > drain:
                            continue
                        prior = db.execute('SELECT output FROM completed WHERE id=?', (event['message_id'],)).fetchone()
                        if prior and prior[0] != event['output_sha256']:
                            output_mismatch += 1
                        db.execute('''INSERT INTO completed VALUES (?,?,1,?,?,?) ON CONFLICT(id)
                                      DO UPDATE SET started=CASE WHEN excluded.finished < finished THEN excluded.started ELSE started END,
                                      processing=CASE WHEN excluded.finished < finished THEN excluded.processing ELSE processing END,
                                      finished=min(finished,excluded.finished), attempts=attempts+1''',
                                   (event['message_id'], timestamp, event['output_sha256'], event.get('processing_start_timestamp'), event.get('processing_seconds')))
                    elif kind in ('delivery_failed', 'enqueue_failed', 'enqueue_cancelled'):
                        failed_sends += 1
                    elif kind == 'delivery_summary':
                        unresolved_sends += event['unresolved']
        db.commit()
        admitted = db.execute('SELECT count(*) FROM admitted').fetchone()[0]
        missing = late = censored = done = duplicates = valid_completions = 0
        db.execute('CREATE TABLE latency (seconds REAL, start_seconds REAL, processing_seconds REAL)')
        for produced, finished, attempts, started, processing in db.execute('SELECT a.produced,c.finished,c.attempts,c.started,c.processing FROM admitted a LEFT JOIN completed c ON a.id=c.id'):
            if finished is None:
                missing += 1
                if produced + deadline <= drain:
                    late += 1
                else:
                    censored += 1
            else:
                done += 1
                duplicates += max(0, attempts - 1)
                delay = finished - produced
                if not math.isfinite(delay) or delay < 0:
                    bad_clock += 1
                else:
                    valid_completions += 1
                    start_delay = started-produced if started is not None else None
                    if start_delay is not None and (not math.isfinite(start_delay) or not 0 <= start_delay <= delay):
                        start_delay = None
                    if processing is not None and (not math.isfinite(processing) or processing < 0):
                        processing = None
                    db.execute('INSERT INTO latency VALUES (?,?,?)', (delay,start_delay,processing))
                    if latency_sink is not None:
                        latency_sink(delay)
                    late += delay > deadline
        latency_sum = db.execute('SELECT coalesce(sum(seconds),0) FROM latency').fetchone()[0]
        db.execute('CREATE INDEX ordered_latency ON latency(seconds)')
        percentiles = quantiles(db, 'latency', 'seconds')
        diagnostics = {}
        for column in ('start_seconds', 'processing_seconds'):
            number, total = db.execute(f'SELECT count({column}),sum({column}) FROM latency').fetchone()
            diagnostics[column] = dict(observation_count=number, mean=total/number if number else None,
                                       **quantiles(db, 'latency', column))
        if unresolved_sends:
            invalid.append('Unresolved sends prevent complete admission accounting')
        if bad_clock:
            invalid.append('Negative or invalid cross-machine latency')
        if output_mismatch:
            invalid.append('Replayed message produced inconsistent outputs')
        partitions = partition_metrics(directory, manifest, partition_latency_sink)
        invalid.extend(partitions['validity_failures'])
        return dict(run_id=manifest['run_id'], validity_failures=sorted(set(invalid)),
                    useful_throughput_per_second=db.execute('SELECT count(*) FROM window_completed').fetchone()[0] / (end-start) if end > start else None,
                    evaluation_seconds=end-start,
                    admitted_messages_per_second=admitted/(end-start) if end > start else None,
                    observed_completion_deadline_miss_fraction=late_attempts_window/valid_attempts_window if valid_attempts_window else None,
                    valid_completion_attempts_in_evaluation=valid_attempts_window,
                    diagnostic_cohort_latencies=diagnostics,
                    admitted_evaluation_cohort=admitted, completed_by_drain=done, incomplete_by_drain=missing,
                    deadline_misses=late, deadline_censored=censored,
                    deadline_miss_fraction=late / admitted if admitted and not censored and not bad_clock else None,
                    duplicate_completion_attempts=duplicates, invalid_clock_observations=bad_clock,
                    failed_or_cancelled_sends_whole_run=failed_sends, unresolved_sends_whole_run=unresolved_sends,
                    completed_attempts_per_second=completed_attempts_window / (end - start) if end > start else None,
                    admitted_cohort_valid_completion_count=valid_completions,
                    admitted_cohort_completion_latency_sum_seconds=latency_sum,
                    admitted_cohort_completion_mean_seconds=latency_sum / valid_completions if valid_completions else None,
                    admitted_cohort_completion_p50_seconds=percentiles['p50'],
                    admitted_cohort_completion_p95_seconds=percentiles['p95'],
                    admitted_cohort_completion_p99_seconds=percentiles['p99'],
                    partition_metrics=partitions,
                    incomplete_fraction=missing / admitted if admitted else None,
                    capacity_estimate=manifest.get('capacity_estimate'),
                    notes=['Mean and p50/p95/p99 exclude unfinished messages; always report incomplete/deadline results alongside them.',
                           'Percentiles use nearest rank among valid cohort completions by the drain bound.',
                           'Start/processing diagnostic counts may be lower for older evidence without those fields.',
                           'Clock uncertainty still needs a Nautilus measurement.',
                           'Histograms and final counters remain available in the Prometheus export and final.prom files.'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    args = parser.parse_args()
    result = evaluate(args.run_directory)
    output = args.run_directory / 'outcome-summary.json'
    output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    if result['validity_failures']:
        raise SystemExit(2)
