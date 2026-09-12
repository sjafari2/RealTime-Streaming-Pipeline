#!/usr/bin/env python3
"""Re-evaluate collected runs and summarize matching configurations without averaging away failures."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import tempfile

from evaluate_run import evaluate
from analyze_lag import analyze
from analyze_execution import analyze as execution_analysis
from partition_outcomes import quantiles


METRICS = ('admitted_cohort_completion_mean_seconds', 'admitted_cohort_completion_p50_seconds',
           'admitted_cohort_completion_p95_seconds', 'admitted_cohort_completion_p99_seconds',
           'deadline_miss_fraction', 'incomplete_fraction', 'useful_throughput_per_second',
           'admitted_messages_per_second', 'completed_attempts_per_second', 'duplicate_completion_attempts')

PARTITION_METRICS = ('completion_mean_seconds', 'completion_p50_seconds', 'completion_p95_seconds',
                     'completion_p99_seconds', 'admitted_per_second', 'acknowledged_arrivals_per_second',
                     'unique_completions_per_second', 'completion_attempts_per_second',
                     'incomplete_fraction', 'deadline_miss_fraction')


def repetition_stats(values):
    values = [v for v in values if v is not None and math.isfinite(v)]
    return dict(n=len(values), mean=statistics.mean(values) if values else None,
                sample_stddev=statistics.stdev(values) if len(values) > 1 else None,
                minimum=min(values) if values else None, maximum=max(values) if values else None)


def configuration(directory, manifest):
    # Run IDs and generated topic names identify repetitions, not workload settings.
    config = {k: v for k, v in manifest['config'].items()
              if k not in ('RUN_ID', 'TOPIC_TITLE', 'PRODUCER_POD_COUNT', 'CONSUMER_POD_COUNT')}
    signatures = {}
    for role in ('producer', 'consumer'):
        observed = set()
        for path in (directory / role).rglob('events.jsonl'):
            with path.open() as stream:
                for line in stream:
                    row = json.loads(line)
                    if row.get('event') == 'started':
                        observed.add(json.dumps({k: row.get(k) for k in ('source_sha256', 'packages', 'python_version')}, sort_keys=True))
        signatures[role] = [json.loads(value) for value in sorted(observed)]
    prefix = manifest['config'].get('TOPIC_TITLE', '')
    assignment = sorted((r['pod'], r['topic'][len(prefix):] if r['topic'].startswith(prefix) else r['topic'], r['partition'])
                        for r in manifest.get('initial_assignment', []))
    return dict(config=config, initial_producers=len(manifest['producer_pods']),
                initial_consumers=len(manifest['consumer_pods']), runtime_signatures=signatures,
                intervention={k:v for k,v in manifest.get('intervention', {}).items() if k != 'at_epoch'},
                initial_assignment=assignment,
                initial_consumer_resources=manifest.get('initial_consumer_resources'),
                evaluation_seconds=manifest['producer_end_epoch']-manifest['evaluation_start_epoch'],
                followup_seconds=manifest['drain_end_epoch']-manifest['producer_end_epoch'])


def summarize(directories):
    groups = {}
    seen = set()
    with tempfile.TemporaryDirectory() as temporary:
        db = sqlite3.connect(str(Path(temporary) / 'latencies.sqlite'))
        db.execute('CREATE TABLE latency (group_id TEXT, run_id TEXT, seconds REAL)')
        db.execute('CREATE TABLE partition_latency (group_id TEXT, run_id TEXT, topic_index INTEGER, partition INTEGER, seconds REAL)')
        for directory in directories:
            directory = Path(directory)
            manifest = json.loads((directory / 'manifest.json').read_text())
            run_id = manifest['run_id']
            if run_id in seen:
                raise ValueError('Duplicate RUN_ID; refusing to double-count: ' + run_id)
            seen.add(run_id)
            settings = configuration(directory, manifest)
            key = hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest()
            group = groups.setdefault(key, dict(group_id=key, settings=settings, runs=[], excluded_runs=[]))
            result = evaluate(directory, latency_sink=lambda value: db.execute(
                'INSERT INTO latency VALUES (?,?,?)', (key, run_id, value)),
                partition_latency_sink=lambda topic, part, value: db.execute(
                    'INSERT INTO partition_latency VALUES (?,?,?,?,?)', (key, run_id, topic, part, value)))
            status_path = directory / 'runner-status.json'
            if status_path.exists() and json.loads(status_path.read_text()).get('status') != 'complete':
                result['validity_failures'].append('Runner marked the experiment incomplete or failed')
            # Failed evidence must not silently contribute even if it contains valid-looking samples.
            if result['validity_failures']:
                db.execute('DELETE FROM latency WHERE run_id=?', (run_id,))
                db.execute('DELETE FROM partition_latency WHERE run_id=?', (run_id,))
                group['excluded_runs'].append(dict(run_id=run_id, directory=str(directory.resolve()),
                                                   validity_failures=result['validity_failures']))
            else:
                lag = None
                if (directory/'prometheus.json').exists():
                    lag = analyze(directory)
                    result['lag_metrics'] = {name: lag[name] for name in
                        ('covered_fraction','time_weighted_mean_lag','backlog_area_offset_seconds','peak_sampled_lag','final_lag')}
                else:
                    result['lag_metrics'] = None
                result['execution_metrics'] = execution_analysis(directory, lag)
                group['runs'].append(result)
        db.execute('CREATE INDEX ordered_latency ON latency(group_id,seconds)')
        db.execute('CREATE INDEX ordered_partition_latency ON partition_latency(group_id,topic_index,partition,seconds)')
        for key, group in groups.items():
            runs = group['runs']
            count, total = db.execute('SELECT count(*),coalesce(sum(seconds),0) FROM latency WHERE group_id=?', (key,)).fetchone()
            percentiles = quantiles(db, 'latency', 'seconds', 'group_id=?', (key,))
            admitted = sum(row['admitted_evaluation_cohort'] for row in runs)
            unfinished = sum(row['incomplete_by_drain'] for row in runs)
            misses = sum(row['deadline_misses'] for row in runs)
            censored = sum(row['deadline_censored'] for row in runs)
            group['included_run_count'] = len(runs)
            group['run_level'] = {metric: repetition_stats([row[metric] for row in runs]) for metric in METRICS}
            group['lag_run_level'] = {name: repetition_stats([
                row['lag_metrics'][name] for row in runs if row.get('lag_metrics')])
                for name in ('covered_fraction','time_weighted_mean_lag','backlog_area_offset_seconds','peak_sampled_lag','final_lag')}
            group['pooled_messages'] = dict(valid_completion_count=count, latency_sum_seconds=total,
                completion_mean_seconds=total/count if count else None,
                **{f'completion_{name}_seconds': value for name,value in percentiles.items()},
                admitted_messages=admitted, incomplete_messages=unfinished,
                incomplete_fraction=unfinished/admitted if admitted else None,
                deadline_misses=misses, deadline_censored=censored,
                deadline_miss_fraction=misses/admitted if admitted and not censored else None)
            partition_rows = {}
            for row in runs:
                for partition in row['partition_metrics']['partitions']:
                    partition_rows.setdefault((partition['topic_index'], partition['partition']), []).append(partition)
            group['partitions'] = []
            for (topic, partition), rows in sorted(partition_rows.items()):
                count = sum(r['valid_completion_count'] for r in rows)
                total = sum(r['completion_latency_sum_seconds'] for r in rows)
                admitted = sum(r['admitted_messages'] for r in rows)
                unfinished = sum(r['incomplete_messages'] for r in rows)
                late = sum(r['deadline_misses'] for r in rows)
                censored = sum(r['deadline_censored'] for r in rows)
                group['partitions'].append(dict(topic_index=topic, partition=partition, available_runs=len(rows),
                    run_level={m: repetition_stats([r[m] for r in rows]) for m in PARTITION_METRICS},
                    pooled_messages=dict(valid_completion_count=count, latency_sum_seconds=total,
                        completion_mean_seconds=total/count if count else None,
                        **{f'completion_{name}_seconds': value for name,value in quantiles(db, 'partition_latency', 'seconds',
                            'group_id=? AND topic_index=? AND partition=?', (key, topic, partition)).items()},
                        admitted_messages=admitted, incomplete_messages=unfinished,
                        incomplete_fraction=unfinished/admitted if admitted else None,
                        deadline_misses=late, deadline_censored=censored,
                        deadline_miss_fraction=late/admitted if admitted and not censored else None)))
            group['execution_run_level'] = {'consumer_process_seconds': repetition_stats([
                r['execution_metrics']['consumer_process_seconds'] for r in runs])}
            for name in ('covered_fraction', 'scheduled_consumer_pod_seconds_observed',
                         'consumer_container_requested_cpu_seconds_observed', 'consumer_container_requested_gib_seconds_observed'):
                group['execution_run_level'][name] = repetition_stats([
                    (r['execution_metrics']['resource_requests'] or {}).get(name) for r in runs])
            recoveries = [r['execution_metrics']['recovery'] for r in runs if r['execution_metrics']['recovery']]
            group['recovery_run_level'] = dict(measured_runs=len(recoveries),
                censored_runs=sum(r['censored'] for r in recoveries),
                seconds_to_confirmation_observed_only=repetition_stats([r['seconds_to_confirmation'] for r in recoveries]))
        db.close()
    return dict(groups=list(groups.values()), notes=[
        'Pooled mean weights completed messages; run-level mean weights repetitions equally.',
        'Pooled p50/p95/p99 use individual valid cohort completion latencies, not averages of run percentiles.',
        'Means and p99 are conditional on completion by drain. Report incomplete and deadline results too.',
        'Runs with detected evidence validity failures are excluded and listed explicitly.',
        'Groups match configuration, initial replica counts/recorded assignments, declared intervention, observation durations and runtime/source signatures.',
        'Per-partition groups normalize generated topic prefixes to topic indices; unavailable older evidence is not counted as zero.',
        'Resource request-time summaries must be read with coverage; recovery means condition on observed recovery and list censoring.',
        'This does not establish independence, clock accuracy, identical node placement or identical scaling interventions.',
        'Record distinct policies/interventions in EXP_ID; inspect manifests before treating runs as repetitions.',
        'WORKLOAD_SEED is retained in grouping. No confidence interval is claimed; sample standard deviation needs at least two runs.',
        'Per-run summaries are recomputed from evidence; input run directories are not modified.'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directories', type=Path, nargs='+', help='Collected run directories, each containing manifest.json')
    parser.add_argument('--output', type=Path, required=True, help='Output JSON file; existing files are replaced')
    args = parser.parse_args()
    directories = [p.resolve() for p in args.run_directories]
    output = args.output.resolve()
    if any(output == p or p in output.parents for p in directories):
        parser.error('Choose an output outside the input run directories to preserve their evidence.')
    result = summarize(directories)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    print('Summary:', output)
    for group in result['groups']:
        print(group['group_id'][:12], 'included:', group['included_run_count'], 'excluded:', len(group['excluded_runs']))
    if any(group['excluded_runs'] for group in result['groups']):
        raise SystemExit(2)
