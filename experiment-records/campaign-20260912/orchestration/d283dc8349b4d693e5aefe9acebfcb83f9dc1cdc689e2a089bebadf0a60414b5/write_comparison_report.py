"""Write an evidence-linked descriptive report, without inferential claims."""
import argparse,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('directory',type=Path);a=p.parse_args();D=a.directory
R=Path('/Users/soheila/Desktop/Thesis-26-27/code')
s=json.loads((D/'comparison-summary.json').read_text());index=json.loads((D/'index.json').read_text())
lines=['# Preliminary action comparison','',f"Status: **{index.get('status','recorded pairs')}**. Each pair below uses its own matched workload seed. Short, sustained, concentrated-partition and low-input families are analyzed separately.",'']
fields=[('Distinct evaluation admissions','admitted_evaluation_cohort',1,0),('Unfinished at drain','incomplete_by_drain',1,0),('Unfinished fraction (%)','unfinished_fraction',100,3),('Recorded completed-cohort mean (s)','admitted_cohort_completion_mean_seconds',1,3),('Recorded completed-cohort p99 (s)','admitted_cohort_completion_p99_seconds',1,3),('Admissions / evaluation second','admitted_messages_per_second',1,3),('Unique in-evaluation completions / second','useful_throughput_per_second',1,3),('Covered-interval mean processing backlog (offsets)','mean_processing_backlog_offsets',1,1),('Lag coverage (%)','lag_covered_fraction',100,2),('Requested consumer CPU (core-minutes, evaluation + drain)','evaluation_and_drain_requested_cpu_seconds',1/60,3),('Resource coverage for that horizon (%)','evaluation_and_drain_resource_coverage',100,2)]
for pair in s['pairs']:
 n,c=pair['runs']['none'],pair['runs']['scale'];lines += [f"## Pair {pair['pair']} — seed {n['workload_seed']}",'',f"Keep three consumers: `{n['run_id']}`. Schedule three to six: `{c['run_id']}`.",'','| Measure | Keep 3 | Schedule 3 → 6 |','|---|---:|---:|']
 for title,key,factor,digits in fields:
  values=[r['metrics'].get(key) for r in (n,c)]
  formatted=[f'{v*factor:,.{digits}f}' if v is not None else 'Unavailable' for v in values]
  lines.append('| '+title+' | '+' | '.join(formatted)+' |')
 lines += ['',f"Compatibility checks: {pair['compatibility_failures'] or 'passed (workload configuration, action settings and recorded application/Python/package signatures)'}. Both full message-outcome evidence checks passed: {n['evidence_valid'] and c['evidence_valid']}."]
 for action,row in [('Keep 3',n),('Scale',c)]:
  lines += [f"{action} quality flags: {('; '.join(row['quality_flags'])) or 'none at the predeclared coverage screens'}. Recovery: `{json.dumps(row.get('recovery'),sort_keys=True)}`."]
 if c['timing'].get('new_consumers'):
  lines += ['','| New consumer | Node | Request → Kubernetes Ready (s) | Request → first recorded completion (s) |','|---|---|---:|---:|']
  for row in c['timing']['new_consumers']:
   fmt=lambda v:'Unavailable' if v is None else f'{v:.2f}'
   lines.append(f"| {row['pod']} | {row['node']} | {fmt(row['request_to_kubernetes_ready_seconds'])} | {fmt(row['request_to_first_completion_seconds'])} |")
 if index.get('family')=='isolated':
  lines += ['', '| Condition | Initial partition-0 owner | Initial owner node | Partition-0 admissions / s | Partition-0 completions / evaluation s | Partition-0 unfinished | Other-partition unfinished |', '|---|---|---|---:|---:|---:|---:|']
  for action,row in [('Keep 3',n),('Scale',c)]:
   directory=Path(row['directory']);manifest=json.loads((directory/'manifest.json').read_text());outcomes=json.loads((directory/'outcome-summary.json').read_text())
   assignment=next(z for z in manifest['initial_assignment'] if z['partition']==0 and z['topic']==manifest['config']['TOPIC_TITLE']+'_0')
   resource=next(z for z in manifest['initial_consumer_resources'] if z['pod']==assignment['pod'])
   partitions=outcomes['partition_metrics']['partitions'];hot=next(z for z in partitions if z['partition']==0)
   other=sum(z['incomplete_messages'] for z in partitions if z['partition']!=0)
   lines.append(f"| {action} | {assignment['pod']} | {resource['node']} | {hot['admitted_per_second']:.3f} | {hot['unique_completions_per_second']:.3f} | {hot['incomplete_messages']:,} | {other:,} |")
  lines += ['', 'The paired workload seed fixes the generated routing distribution, not the initial Kafka assignment or node capacity. A hot-owner change can also change available processing capacity. The separate one-partition calibration is one recorded-node check, not a capacity constant valid for every node or time.', '', f"![Pair {pair['pair']} partition detail](pair-{pair['pair']}-partition-0.png)", '']
  details=json.loads((D/f"pair-{pair['pair']}-partition-0-data.json").read_text())
  lines += ['| Condition | Observed partition-0 owner / process prefix | Recorded node(s) | First valid evaluation second | Last valid evaluation second |', '|---|---|---|---:|---:|']
  for action,row in [('none',n),('scale',c)]:
   directory=Path(row['directory']);manifest=json.loads((directory/'manifest.json').read_text())
   resources=[json.loads(line) for line in (directory/'resource-history.jsonl').read_text().splitlines() if line.strip()]
   for period in details['runs'][action]['hot_owner_observation_periods']:
    pod,incarnation=period['owner'].split('/',1);first=period['first_valid_evaluation_second'];last=period['last_valid_evaluation_second'];start=manifest['evaluation_start_epoch']
    nodes=sorted({z['node'] for snap in resources if snap.get('valid') and start+first-15<=snap['timestamp']<=start+last+15 for z in snap['pods'] if z['pod']==pod and z.get('node')})
    lines.append(f"| {action} | `{pod}/{incarnation[:8]}` | {', '.join(nodes) or 'Unavailable'} | {first:.2f} | {last:.2f} |")
  lines += ['', 'These intervals locate valid partition-owner observations, not exact handover times; invalid gaps break intervals even if the same owner returns. Node names come from resource observations within the displayed interval plus a 15-second sampling margin. Full process identities and source hashes are in the partition plot data JSON. Multiple recorded nodes would be shown explicitly.', '']
 for row in (n,c):
  launch_path=Path(row['directory'])/'launch.json'
  if not launch_path.exists(): launch_path=R/'experiment-records'/row['run_id']/'launch.json'
  if launch_path.exists():
   launch=json.loads(launch_path.read_text())
   if launch.get('git_status','').strip():
    lines += ['',f"Source provenance: `{row['run_id']}` launched from Git revision `{launch['git_revision']}` with recorded working-tree changes. Its launch record retains the exact status; the matched application signatures are checked separately."]
    if row['run_id']=='run-20260912-070155':
     lines += ['The uncommitted changes were the single/batch HPA pause-and-restore wrapper, its documentation and tests, later committed as `6644c86`. They were present before this launcher started. This campaign already owned its HPA pause and used the direct scheduled-run path; the producer/consumer application code matched the paired run. The later commit is not represented as the revision that existed at launch.']
 lines += ['',f"![Pair {pair['pair']} monitoring curves](pair-{pair['pair']}-timecourse.png)",'']
lines += ['## Descriptive differences across matched seeds','',
 'Each difference is scale minus keep-3 for one matched seed. Means and sample standard deviations below summarize these paired run-level differences; they do not pool message latencies. Coverage screens apply separately to each metric.','',
 '| Measure | Eligible pairs | Mean paired difference | Minimum | Maximum | Sample SD |','|---|---:|---:|---:|---:|---:|']
for title,key,factor,digits in fields:
 if key not in ('unfinished_fraction','admitted_cohort_completion_mean_seconds','admitted_cohort_completion_p99_seconds','useful_throughput_per_second','mean_processing_backlog_offsets','evaluation_and_drain_requested_cpu_seconds'):continue
 d=s['descriptive_paired_differences'].get(key)
 if not d:
  lines.append(f'| {title} | 0 | Unavailable | Unavailable | Unavailable | Unavailable |');continue
 values=[d[k] for k in ('mean_paired_difference','minimum','maximum','sample_standard_deviation')]
 fmt=[f'{v*factor:,.{digits}f}' if v is not None else 'Unavailable' for v in values]
 lines.append('| '+title+' | '+str(d['pair_count'])+' | '+' | '.join(fmt)+' |')
lines += ['', 'Unfinished-fraction differences are percentage points. With one pair, sample SD is unavailable; with two, it is only a descriptive spread estimate. No confidence interval or significance test is claimed.','']
lines += ['## Interpretation limits','',
 'Completed-message mean and percentiles are conditional on completion by the fixed drain. Read them with unfinished counts; missing completion latency is never assigned zero. A run p99 or mean of run p99 values is not a pooled-message percentile.',
 'Backlog integrals and means cover only eligible observed intervals. Missing or ownership-transition intervals are not zero, and different coverage can affect the comparison. The displayed offset backlog is not the unique unfinished cohort count.',
 'Cost here is requested consumer-container CPU time, with a common evaluation-through-drain horizon. It is not measured whole-cluster CPU use or a billing total. Startup timing mixes coordinator, Kubernetes and application timestamps; clock and sampling uncertainty remain.',
 'A threshold-hold confirmation means recovery from overload only when backlog was initially above the threshold. In an already healthy low-input trial it is a health check; missing transition observations do not establish failed recovery.',
 'These are short preliminary scheduled-action comparisons on shared nodes, not a tuned HPA evaluation or a completed adaptive-selector study. Ordinary initial assignments and node placements are recorded, not forced equal. Two seed pairs do not justify a strong confidence interval or broad causal/general superiority claim.',
 'Earlier guard-interrupted and pre-production diagnostics remain retained separately. The 99 ms threshold is provisional and the clock probes do not establish its accuracy as an SLA test.',
 '', '![Paired whole-run outcomes](paired-outcomes.png)','',
 'Raw per-message evidence remains under the active code results directory and on Nautilus PVCs. The individual Git run records include evidence hashes, configurations, outcomes, source provenance and monitoring plots. This summary does not replace the raw evidence backup.','']
(D/'comparison-report.md').write_text('\n'.join(lines))
print(D/'comparison-report.md')
