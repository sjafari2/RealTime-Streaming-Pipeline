"""Compare explicitly paired runs without pooling away replication or censoring."""
import argparse
from collections import Counter
from datetime import datetime
import csv
import json
import math
from pathlib import Path
import statistics
import sys

from analyze_execution import resource_integral
from evidence_io import event_paths, open_events


def processing_integral(snapshots, start, end):
    area = covered = 0.0
    for left, right in zip(snapshots, snapshots[1:]):
        # The lag analyzer already checks validity, ownership, offset monotonicity
        # and the maximum sample gap before reporting an interval's growth.
        if not left['valid'] or not right['valid'] or right.get('growth_offsets_per_second') is None:
            continue
        t0, t1 = left['timestamp'], right['timestamp']
        a, b = max(start, t0), min(end, t1)
        y0, y1 = left.get('processing_backlog'), right.get('processing_backlog')
        if b <= a or not all(isinstance(y, (int, float)) and math.isfinite(y) for y in (y0, y1)):
            continue
        ya = y0 + (y1-y0)*(a-t0)/(t1-t0)
        yb = y0 + (y1-y0)*(b-t0)/(t1-t0)
        area += .5*(ya+yb)*(b-a)
        covered += b-a
    return dict(area_offset_seconds=area if covered else None, covered_seconds=covered,
                covered_fraction=covered/(end-start), mean_offsets=area/covered if covered else None)


def run_record(directory):
    d = Path(directory)
    read = lambda name: json.loads((d / name).read_text())
    m, status, o, lag, e = [read(n) for n in ('manifest.json', 'runner-status.json', 'outcome-summary.json', 'lag-summary.json', 'execution-summary.json')]
    config = m['config']
    plan = m.get('intervention', {})
    start, end, drain = (m[k] for k in ('evaluation_start_epoch', 'producer_end_epoch', 'drain_end_epoch'))
    anchor = plan.get('at_epoch', start)
    metrics = {k: o[k] for k in ('admitted_evaluation_cohort', 'completed_by_drain', 'incomplete_by_drain',
               'duplicate_completion_attempts', 'admitted_messages_per_second', 'useful_throughput_per_second',
               'deadline_miss_fraction', 'admitted_cohort_completion_mean_seconds',
               'admitted_cohort_completion_p50_seconds', 'admitted_cohort_completion_p95_seconds',
               'admitted_cohort_completion_p99_seconds')}
    metrics['unfinished_fraction'] = o['incomplete_by_drain']/o['admitted_evaluation_cohort'] if o['admitted_evaluation_cohort'] else None
    metrics['target_admission_fraction'] = o['admitted_messages_per_second']/(float(config['TARGET_RATE'])*int(config['PRODUCER_POD_COUNT']))
    metrics['lag_covered_fraction'] = lag['covered_fraction']
    metrics['consumer_process_seconds'] = e['consumer_process_seconds']
    areas = {name: processing_integral(lag['snapshots'], a, b) for name, a, b in
             [('evaluation', start, end), ('after_scheduled_action', anchor, end)]}
    metrics['processing_backlog_area_offset_seconds'] = areas['evaluation']['area_offset_seconds']
    metrics['mean_processing_backlog_offsets'] = areas['evaluation']['mean_offsets']
    samples = [json.loads(line) for line in (d/'resource-history.jsonl').read_text().splitlines()]
    windows = {name: resource_integral(samples, a, b, float(config.get('RESOURCE_MAX_GAP_SECONDS', 15)))
               for name, a, b in [('production', m['start_epoch'], end), ('evaluation', start, end),
                                  ('evaluation_and_drain', start, drain), ('after_scheduled_action', anchor, end)]}
    cost = windows['evaluation_and_drain']
    metrics['evaluation_and_drain_requested_cpu_seconds'] = cost['consumer_container_requested_cpu_seconds_observed']
    metrics['evaluation_and_drain_resource_coverage'] = cost['covered_fraction']
    # Record application hashes and package sets; a collector Git commit does not
    # retroactively identify code that executed in an older run.
    sources = set()
    commits = Counter()
    commit_errors = []
    for path in event_paths(d):
        final = json.loads(path.with_name('final.json').read_text())
        with open_events(path) as stream:
            for line in stream:
                event = json.loads(line)
                if event['event'] == 'started':
                    sources.add(json.dumps(dict(role=final['role'], python=event.get('python_version'),
                                                packages=event.get('packages'), source=event.get('source_sha256')), sort_keys=True))
                elif event['event'] == 'commit_result':
                    key = 'successful' if event['success'] else 'group_transition_failure' if event.get('group_transition') else 'other_failure'
                    commits[key] += 1
                    if not event['success']:
                        commit_errors.append(dict(event, pod=final['pod']))
    timing = dict(scheduled_epoch=plan.get('at_epoch'), action=plan.get('action', 'none'))
    decisions = [x for x in e['intervention_events'] if x['event'] == 'decision']
    if decisions:
        decision = decisions[0]['timestamp']
        timing['decision_delay_seconds'] = decision-anchor
        initial = set(m['consumer_pods'])
        new = [x for x in e.get('observed_consumer_pods', []) if x['pod'] not in initial]
        readiness = []
        for pod in new:
            transition = pod.get('ready_transition')
            at = datetime.fromisoformat(transition.replace('Z', '+00:00')).timestamp() if transition else None
            useful = [x['first_completion_epoch'] for x in e['assignment_first_completions']
                      if x['pod'] == pod['pod'] and x['first_completion_epoch'] is not None and x['first_completion_epoch'] >= decision]
            observations = [x['timestamp'] for x in e['intervention_events']
                            if x['event'] == 'pod_ready_observed' and x['uid'] == pod['uid']]
            readiness.append(dict(pod=pod['pod'], node=pod['node'],
                request_to_kubernetes_ready_seconds=at-decision if at is not None else None,
                request_to_ready_observed_seconds=min(observations)-decision if observations else None,
                request_to_first_completion_seconds=min(useful)-decision if useful else None))
        timing['new_consumers'] = readiness
    flags = []
    if status['status'] != 'complete' or o['validity_failures']:
        flags.append('Run did not complete its evidence checks')
    if metrics['target_admission_fraction'] < .98:
        flags.append('Admitted evaluation traffic below 98% of target')
    if lag['covered_fraction'] < .9:
        flags.append('Lag coverage below 90%; transition gaps remain part of the result')
    if cost['covered_fraction'] < .95:
        flags.append('Common-horizon resource coverage below 95%; do not claim cost superiority')
    return dict(run_id=m['run_id'], directory=str(d.resolve()), action=plan.get('action', 'none'),
                workload_seed=config['WORKLOAD_SEED'], config=config, intervention=plan, metrics=metrics,
                processing_backlog_integrals=areas, resource_windows=windows, timing=timing,
                recovery=e.get('recovery'), commit_results=dict(commits), commit_errors=commit_errors,
                source_signatures=sorted(sources), quality_flags=flags, evidence_valid=status['status']=='complete' and not o['validity_failures'], validity_failures=o['validity_failures'])


def summarize(index, results_root):
    pairs = []
    for pair in index['pairs']:
        rows = {action: run_record(Path(results_root)/pair[action]) for action in ('none', 'scale')}
        left, right = rows['none'], rows['scale']
        ignored = {'RUN_ID', 'TOPIC_TITLE', 'EXP_ID'}
        compare_config = lambda row: {k:v for k,v in row['config'].items() if k not in ignored}
        mismatches = []
        if compare_config(left) != compare_config(right): mismatches.append('Frozen workload configurations differ')
        if left['source_signatures'] != right['source_signatures']: mismatches.append('Application source, Python or package signatures differ')
        if left['action'] != 'none' or right['action'] != 'scale': mismatches.append('Unexpected recorded action')
        for field in ('after_evaluation_start_seconds', 'initial_consumers', 'recovery_threshold_offsets', 'recovery_hold_seconds'):
            if left['intervention'].get(field) != right['intervention'].get(field): mismatches.append('Intervention protocol differs: '+field)
        delta = {k: right['metrics'][k]-v for k,v in left['metrics'].items()
                 if isinstance(v, (int, float)) and isinstance(right['metrics'].get(k), (int, float))}
        pairs.append(dict(pair=pair['pair'], runs=rows, compatibility_failures=mismatches, scale_minus_none=delta))
    # Pair effects from different durations or processing configurations do not
    # belong to one replication distribution. Use one index per condition family.
    families = set()
    provenance = set()
    for pair in pairs:
        if pair['compatibility_failures']:
            continue
        for row in pair['runs'].values():
            families.add(json.dumps({k:v for k,v in row['config'].items()
                                    if k not in {'RUN_ID', 'TOPIC_TITLE', 'EXP_ID', 'WORKLOAD_SEED'}}, sort_keys=True))
            provenance.add(tuple(row['source_signatures']))
    if len(families) > 1 or len(provenance) > 1:
        raise ValueError('Use a separate comparison index for each workload, duration and application version family')
    distribution = {}
    for pair in pairs:
        if pair['compatibility_failures'] or any(not r.get('evidence_valid', True) for r in pair['runs'].values()):
            continue
        for metric, value in pair['scale_minus_none'].items():
            # Monitoring loss during scaling must not erase an unfavorable,
            # completely reconciled message outcome from the comparison.
            if metric in ('processing_backlog_area_offset_seconds', 'mean_processing_backlog_offsets'):
                if any(r['metrics'].get('lag_covered_fraction', 0)<.9 for r in pair['runs'].values()):
                    continue
            if metric == 'evaluation_and_drain_requested_cpu_seconds':
                if any(r['metrics'].get('evaluation_and_drain_resource_coverage', 0)<.95 for r in pair['runs'].values()):
                    continue
            distribution.setdefault(metric, []).append(value)
    descriptive = {k:dict(pair_count=len(v), mean_paired_difference=statistics.mean(v),
                         minimum=min(v), maximum=max(v), sample_standard_deviation=statistics.stdev(v) if len(v)>1 else None)
                   for k,v in distribution.items()}
    return dict(pairs=pairs, descriptive_paired_differences=descriptive, notes=[
        'Every pair and quality flag remains visible. Completed evidence-valid compatible pairs contribute message outcomes even when monitoring is incomplete. Coverage screens apply only to backlog and resource aggregates; each metric reports its own pair count.',
        'Low achieved admission is reported alongside outcomes and limits a matched-load causal interpretation; it is not silently removed to improve the treatment result.',
        'Scale-minus-none latency differences compare per-run conditional completion quantiles; unfinished fractions must be read alongside them.',
        'A mean of per-run p99 values is not a pooled message p99. No pooled p99 or confidence interval is claimed.',
        'Processing-backlog integrals use trapezoids only across valid contiguous same-owner monotonic-offset observations; coverage is explicit.',
        'Common resource horizons use evaluation start through drain end and the same declared duration for both conditions. Preparation remains separate.',
        'Resource quantities describe requested consumer-container CPU time, not actual whole-cluster cost.',
        'Scheduled scale-out tests an action, not an automatic selector, a tuned HPA baseline or novelty over prior systems.'])


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('index', type=Path)
    p.add_argument('--results-root', type=Path, default=Path(__file__).resolve().parents[1]/'results')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    result = summarize(json.loads(args.index.read_text()), args.results_root)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output/'comparison-summary.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    rows = [dict(pair=p['pair'], run_id=r['run_id'], action=action, **r['metrics'], quality_flags='; '.join(r['quality_flags']))
            for p in result['pairs'] for action,r in p['runs'].items()]
    with (args.output/'comparison-metrics.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader(); writer.writerows(rows)
    print('Saved', len(rows), 'individual run records and', len(result['pairs']), 'paired comparisons')
