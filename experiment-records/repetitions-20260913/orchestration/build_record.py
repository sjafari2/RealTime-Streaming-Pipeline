"""Build small records from a verified repetition; preserve raw evidence separately."""
from pathlib import Path
import argparse,hashlib,json,shutil
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
p=argparse.ArgumentParser();p.add_argument('audit',type=Path);p.add_argument('name',choices=['8020','single-partition']);a=p.parse_args()
A=a.audit;R=Path('/Users/soheila/Desktop/Thesis-26-27/code');S=A/'records-staged';B=S/'experiment-records/repetitions-20260913'/a.name
read=lambda p:json.loads(p.read_text())
b=read(A/'block/block-status.json');assert b['status']=='complete' and b['restoration']=='verified'
assert read(A/'final-runtime-check.json')['all_applications_stopped']
assert read(A/'evidence-backup-manifest.json')['verified_round_trip']
x=read(A/'comparison/comparison-summary.json')['pairs'][0];assert not x['compatibility_failures']
B.mkdir(parents=True,exist_ok=False)
for f in (A/'block').iterdir():
 if f.is_file():shutil.copy2(f,B/f.name)
for name in ['final-runtime-check.json','comparison-index.json','evidence-backup-manifest.json']:
 shutil.copy2(A/name,B/name)
for name in ['comparison','plots','transitions']:shutil.copytree(A/name,B/name)
rows=[];verified=0
for item in b['attempts']:
 d=R/'results'/item['run_id'];o=S/'experiment-records'/item['run_id'];o.mkdir(parents=True)
 v=read(d/'evidence-verification.json');assert v['all_files_match'];verified+=v['file_count']
 for name in ['manifest.json','pipeline-configmap.yaml','runner-status.json','placement-checks.jsonl','preparation-evidence-check.json','evidence-verification.json','outcome-summary.json','lag-summary.json','execution-summary.json','intervention-events.jsonl','resource-history.jsonl','export-status.json']:
  if (d/name).exists():shutil.copy2(d/name,o/name)
 if item['preparation_only']:
  assert read(d/'preparation-evidence-check.json')['valid'];description='Empty preparation verified, with zero experiment message activity.'
 else:
  q=read(d/'outcome-summary.json');assert not q['validity_failures']
  checks=[json.loads(line) for line in (d/'placement-checks.jsonl').read_text().splitlines()]
  assert {c['stage'] for c in checks}>={'before_production','before_intervention'}
  assert all(c['valid'] and c['reference_sha256']==b['reference_sha256'] for c in checks)
  description=f"Completed {q['completed_by_drain']:,} of {q['admitted_evaluation_cohort']:,} evaluation messages by drain; {q['incomplete_by_drain']:,} unfinished."
 (o/'validation-report.md').write_text(f"# {item['run_id']}\n\n{description}\n\nStage: {item['stage']}. PVC/local evidence hashes match.\n")
for action in ['none','scale']:
 run=x['runs'][action];m=run['metrics'];d=R/'results'/run['run_id'];cfg=read(d/'manifest.json')['config']
 assert cfg['SKEW_FRACTION']=='0.8' and cfg['SKEW_PARTITION']==('0.2' if a.name=='8020' else '0')
 hot={5,10,25,33,35,39,43,45,51,55,58,59} if a.name=='8020' else {0}
 parts=read(d/'outcome-summary.json')['partition_metrics']['partitions']
 hot_count=sum(p['admitted_messages'] for p in parts if p['partition'] in hot)
 rows.append(dict(action=action,run_id=run['run_id'],admitted=m['admitted_evaluation_cohort'],completed=m['completed_by_drain'],unfinished=m['incomplete_by_drain'],unfinished_percent=100*m['unfinished_fraction'],completion_p99_seconds=m['admitted_cohort_completion_p99_seconds'],completion_mean_seconds=m['admitted_cohort_completion_mean_seconds'],requested_cpu_core_minutes=m['evaluation_and_drain_requested_cpu_seconds']/60,resource_coverage=m['evaluation_and_drain_resource_coverage'],lag_coverage=m['lag_covered_fraction'],selected_traffic_percent=100*hot_count/m['admitted_evaluation_cohort'],duplicate_completion_attempts=m['duplicate_completion_attempts'],recovery=run['recovery'],timing=run['timing'],quality_flags=run['quality_flags']))
previous=R/'experiment-records'/('8020-comparison-20260913' if a.name=='8020' else 'static-startup-executed-20260912')
old=read(previous/'placement-reference.json');new=read(B/'placement-reference.json')
reference_comparison=dict(previous_record=str(previous.relative_to(R)),same_assignment=old['assignment']==new['assignment'],same_original_pod_identities=old['pods']==new['pods'])
summary=dict(status='complete',workload=b['workload'],code_commit=b['code_commit'],order=b['order'],preparations=4,performance_trials=2,verified_evidence_files=verified,restoration=b['restoration'],all_applications_stopped=True,reference_comparison=reference_comparison,runs=rows,aggregate_backlog_comparison_eligible=all(z['lag_coverage']>=.9 for z in rows))
(B/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none'})
fig,axes=plt.subplots(1,3,figsize=(12,4.3),layout='constrained')
for ax,key,title,label in [(axes[0],'unfinished_percent','Unfinished at drain','Percent of evaluation cohort'),(axes[1],'completion_p99_seconds','Completion p99','Seconds; completed cohort only'),(axes[2],'requested_cpu_core_minutes','Observed requested CPU','Core-minutes; covered intervals only')]:
 vals=[r[key] for r in rows];bars=ax.bar(['Keep 3','Scale to 6'],vals,color=['#355c7d','#d87532']);ax.bar_label(bars,fmt='%.2f',padding=3);ax.set_ylim(0,max(1,max(vals)*1.2));ax.set(title=title,ylabel=label)
title='80% across 12 of 60 partitions' if a.name=='8020' else '80% to partition 0'
coverage_text=' / '.join(f"{100*z['resource_coverage']:.2f}%" for z in rows)
fig.suptitle(title+' · second pair, keep-three first\n1,500 messages/s · 5 minutes production · resource coverage: '+coverage_text,fontsize=12)
fig.savefig(B/'paired-outcomes.png',dpi=180);fig.savefig(B/'paired-outcomes.svg');plt.close(fig)
f=B/'paired-outcomes.svg';f.write_text('\n'.join(line.rstrip() for line in f.read_text().splitlines())+'\n')
text=f'''# {title}: second matched-start comparison

Four empty preparations and both performance trials completed. Keep-three ran first, followed by scale-to-six. Both used seed 71, the same frozen starting ownership and original pod identities within this block. Original settings were restored and application shutdown was verified.

Each trial used three producers at 1,500 messages/s total, 60 partitions, 100-byte payload setting, 2,000 SHA-256 iterations and no artificial sleep. Production lasted 300 seconds including 60 seconds warm-up, followed by 120 seconds drain. The decision was scheduled at production +120 seconds.

| Treatment | Evaluation messages | Unfinished | Completion p99 (s) | Observed requested CPU (core-min) | Lag coverage | Resource coverage |
|---|---:|---:|---:|---:|---:|---:|
'''
for z in rows:text+=f"| {'Keep 3' if z['action']=='none' else 'Scale to 6'} | {z['admitted']:,} | {z['unfinished']:,} ({z['unfinished_percent']:.2f}%) | {z['completion_p99_seconds']:.3f} | {z['requested_cpu_core_minutes']:.2f} | {100*z['lag_coverage']:.2f}% | {100*z['resource_coverage']:.2f}% |\n"
text+='\n![Paired outcomes](paired-outcomes.png)\n\nCompletion is after application work and before commit acknowledgment. P99 includes only distinct acknowledged evaluation messages completed by drain. Read unfinished outcomes alongside latency. Requested CPU is integrated only over observed intervals within the common evaluation-through-drain horizon; it is not an actual monetary bill. With incomplete resource coverage, report the observed amount and coverage rather than a complete-run resource percentage difference.\n\n'
text+=f"Compared with the preceding block: same complete initial assignment = **{reference_comparison['same_assignment']}**; same original pod identities/resources/nodes = **{reference_comparison['same_original_pod_identities']}**. New consumer placement and shared-machine contention remain uncontrolled. These two blocks are repetitions, not proof of a general causal effect.\n\n"
text+=('Both lag coverages meet the 90% screen.\n\n' if summary['aggregate_backlog_comparison_eligible'] else 'At least one lag coverage is below 90%; exclude this pair from aggregate backlog reduction claims. Keep the recorded gaps.\n\n')
for z in rows:text+=f"- {z['action']}: measured selected-partition traffic {z['selected_traffic_percent']:.3f}%; resource coverage {100*z['resource_coverage']:.2f}%; duplicate completion attempts {z['duplicate_completion_attempts']}; recovery status {z['recovery']['status']}.\n"

for z in rows:
 for c in z['timing'].get('new_consumers',[]):
  text+=f"- {c['pod']}: request to first completion {c['request_to_first_completion_seconds']} seconds; node {c['node']}.\n"
text+='\nAll-finished by drain is distinct from recovery during continued input. The 99 ms deadline remains provisional, and cross-node clock uncertainty is not independently measured. No reassignment, key splitting or adaptive selector was tested.\n\n'
text+=f"Executed revision: `{b['code_commit']}`. All {verified} collected evidence files matched their PVC hashes. The verified local raw-evidence archive is described in [backup manifest](evidence-backup-manifest.json); it is not an off-machine backup.\n\n"
text+='[Exact summary](summary.json) · [Paired calculations](comparison/comparison-summary.json) · [Keep-three plots](plots/none/overview.png) · [Scaling plots](plots/scale/overview.png) · [Keep-three commit audit](transitions/none/transition-audit.json) · [Scaling commit audit](transitions/scale/transition-audit.json)\n'
(B/'README.md').write_text(text)
(B/'record-files.json').write_text(json.dumps({str(f.relative_to(B)):hashlib.sha256(f.read_bytes()).hexdigest() for f in B.rglob('*') if f.is_file()},indent=2)+'\n')
print(json.dumps(summary,indent=2),flush=True)
