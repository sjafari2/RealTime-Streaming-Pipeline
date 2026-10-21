"""Keep a compact calibration record while raw evidence stays in backed-up storage."""
import hashlib
import json
from pathlib import Path
import statistics

import argparse
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('run', type=Path)
p.add_argument('diagnostics', type=Path)
p.add_argument('output', type=Path)
a = p.parse_args()
run, work = a.run, a.diagnostics
load = lambda path: json.loads(path.read_text())
manifest = load(run/'manifest.json')
outcome = load(run/'outcome-summary.json')
lag = load(run/'lag-summary.json')
audit = load(run/'measurement-audit.json')
execution = load(run/'execution-summary.json')
assert load(run/'runner-status.json')['status'] in ('complete', 'interrupted_or_failed')
assert not outcome['validity_failures'] and not audit['issues']
before = {r['pod']: r for r in load(work/'cgroup-before.json')['pods']}
after = load(work/'cgroup-after.json')['pods']
begin = {r['pod']: r for r in load(work/'cgroup-evaluation-start.json')['pods']}
end_file = 'cgroup-production-end.json' if (work/'cgroup-production-end.json').exists() else 'cgroup-early-stop.json'
end = {r['pod']: r for r in load(work/end_file)['pods']}
parse = lambda raw: {k: int(v) for k,v in (line.split() for line in raw.splitlines())}
resources = []
for row in after:
    pod = row['pod']; first, last = begin[pod], end[pod]
    assert before[pod]['uid'] == row['uid'] == first['uid'] == last['uid']
    assert before[pod]['container_statuses'][0]['containerID'] == row['container_statuses'][0]['containerID']
    x, y = parse(first['cgroup']['cpu.stat']), parse(last['cgroup']['cpu.stat'])
    elapsed = last['epoch']-first['epoch']
    delta = {key:y[key]-x[key] for key in x}
    events = parse(row['cgroup']['memory.events'])
    resources.append(dict(pod=pod, node=row['node'],
        process_evaluation_metrics={r['metric']:r['windows']['evaluation'] for r in audit['process_resources'] if r['pod']==pod},
        counter_interval_start_relative_evaluation_seconds=first['epoch']-manifest['evaluation_start_epoch'],
        counter_interval_end_relative_evaluation_seconds=last['epoch']-manifest['evaluation_start_epoch'],
        container_cpu_mean_cores_over_counter_interval=delta['usage_usec']/1e6/elapsed,
        throttled_cpu_periods=delta['nr_throttled'], cpu_periods=delta['nr_periods'],
        throttled_period_fraction=delta['nr_throttled']/delta['nr_periods'] if delta['nr_periods'] else None,
        cumulative_throttled_seconds=delta['throttled_usec']/1e6,
        container_peak_memory_bytes_since_start=int(row['cgroup']['memory.peak']) if 'memory.peak' in row['cgroup'] else None,
        container_memory_limit_bytes=int(row['cgroup']['memory.max']),
        memory_event_counts=events,
        restart_counts={c['name']:c['restartCount'] for c in row['container_statuses']}))
valid = [r for r in lag['snapshots'] if r['valid']]
record = dict(run_id=manifest['run_id'], code_revision=(work/'code-revision.txt').read_text().strip(),
    purpose='Separate deployment/resource calibration; not added to the 24 main-proposal trials.',
    outcome={key:value for key,value in outcome.items() if not isinstance(value,dict)},
    lag=dict(coverage_fraction=lag['covered_fraction'],
        first_valid_processing_backlog=valid[0]['processing_backlog'],
        first_valid_relative_evaluation_seconds=valid[0]['timestamp']-manifest['evaluation_start_epoch'],
        last_valid_processing_backlog=valid[-1]['processing_backlog'],
        last_valid_relative_evaluation_seconds=valid[-1]['timestamp']-manifest['evaluation_start_epoch'],
        peak_processing_backlog=max(r['processing_backlog'] for r in valid),
        peak_partition_lag=max(r['max_partition_lag'] for r in valid),
        equal_weight_mean_valid_rolling_backlog=statistics.mean(r['window_mean_backlog'] for r in valid if r['window_mean_backlog'] is not None),
        equal_weight_mean_valid_rolling_skew=statistics.mean(r['window_mean_skew'] for r in valid if r['window_mean_skew'] is not None),
        parameters=lag['parameters']),
    ownership_events_during_production=[dict(event=r['event'],pod=r['pod'],timestamp=r['timestamp']) for r in execution['ownership_events'] if manifest['start_epoch']<=r['timestamp']<manifest['producer_end_epoch']],
    resources=resources, producer_workloads=manifest['producer_workloads'],
    source_sha256={name:hashlib.sha256((run/name).read_bytes()).hexdigest() for name in ['manifest.json','pipeline-configmap.yaml','outcome-summary.json','lag-summary.json','measurement-audit.json','execution-summary.json','prometheus.json','runner-status.json']},
    diagnostic_sha256={name:hashlib.sha256((work/name).read_bytes()).hexdigest() for name in ['cgroup-before.json','cgroup-after.json','cgroup-evaluation-start.json',end_file,'clocks-before.json','clocks-after.json']},
    limitations=['Finite calibration observation. Status and actual phase boundaries are reported explicitly; no infinite-horizon stability claim or isolated resource-effect estimate.',
      'Cgroup counter intervals are timed separately and include a small amount of preparation/drain. They also include diagnostic processes; inspect the recorded boundaries before interpreting the averages.',
      'Container memory peaks are since container startup, include more than application RSS, and were captured before the final evidence transfer. Reused producer containers can include earlier runs and transfers; unavailable kernel peak counters remain null.',
      'Kernel clock diagnostics are not an independently verified bound on cross-node clock error.',
      'All messages completing during drain does not establish stability under continuing input.'])
a.output.write_text(json.dumps(record,indent=2,allow_nan=False)+'\n')
for row in resources:
    print(row['pod'], 'container peak MiB:', round(row['container_peak_memory_bytes_since_start']/2**20,2) if row['container_peak_memory_bytes_since_start'] is not None else 'unavailable',
          'throttle seconds:', row['cumulative_throttled_seconds'],
          'throttled periods %:', round(100*row['throttled_period_fraction'],3))

# Keep the fixed-owner cohort accounting separate from completion-window throughput.
from collections import defaultdict
assert not [r for r in execution['ownership_events'] if manifest['start_epoch']<=r['timestamp']<manifest['drain_end_epoch']]
owner={(r['topic'],r['partition']):r['pod'] for r in manifest['initial_assignment']}
cohorts=defaultdict(lambda:dict(partitions=0,admitted_messages=0,completed_cohort_messages=0,incomplete_messages=0,valid_completion_count=0,completion_latency_sum_seconds=0))
for row in outcome['partition_metrics']['partitions']:
    group=cohorts[owner[row['topic'],row['partition']]]; group['partitions']+=1
    for key in ('admitted_messages','completed_cohort_messages','incomplete_messages','valid_completion_count','completion_latency_sum_seconds'):
        group[key]+=row[key]
for group in cohorts.values():
    group['admission_messages_per_second']=group['admitted_messages']/outcome['evaluation_seconds']
    group['completion_mean_seconds']=group['completion_latency_sum_seconds']/group['valid_completion_count'] if group['valid_completion_count'] else None
record.update(per_consumer_cohort=dict(cohorts),diagnostic_cohort_latencies=outcome['diagnostic_cohort_latencies'],
              correctness=outcome['partition_metrics']['correctness'],stability=load(run/'stability-summary.json'),
              timings_seconds=dict(warmup=manifest['evaluation_start_epoch']-manifest['start_epoch'],
                                   evaluation=manifest['producer_end_epoch']-manifest['evaluation_start_epoch'],
                                   drain=manifest['drain_end_epoch']-manifest['producer_end_epoch']),
              deployed_pods=[dict(pod=p['metadata']['name'],uid=p['metadata']['uid'],node=p['spec']['nodeName'],
                                  containers=[dict(name=c['name'],resources=c['resources']) for c in p['spec']['containers']],
                                  image_ids=[c['imageID'] for c in p['status']['containerStatuses']])
                             for p in load(work/'pods-running.json')['items']],
              node_hardware=load(work/'node-hardware.json'))
record['source_sha256']['stability-summary.json']=hashlib.sha256((run/'stability-summary.json').read_bytes()).hexdigest()
record['diagnostic_sha256'].update({name:hashlib.sha256((work/name).read_bytes()).hexdigest() for name in ['pods-running.json','node-hardware.json']})
a.output.write_text(json.dumps(record,indent=2,allow_nan=False)+'\n')
print('Cohorts:',json.dumps(dict(cohorts),indent=2))

if (work/'early-stop-decision.json').exists():
    record['early_stop']=load(work/'early-stop-decision.json')
record['runner_status']=load(run/'runner-status.json')
a.output.write_text(json.dumps(record,indent=2,allow_nan=False)+'\n')
