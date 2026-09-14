from pathlib import Path
import json,shutil,hashlib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
A=Path(__file__).resolve().parent;R=Path('/Users/soheila/Desktop/Thesis-26-27/code');S=A/'records-staged';B=S/'experiment-records/8020-comparison-20260913'
read=lambda p:json.loads(p.read_text())
b=read(A/'block/block-status.json');assert b['status']=='complete' and b['restoration']=='verified'
assert len(b['attempts'])==6 and len(b['runs'])==2
assert read(A/'final-runtime-check.json')['all_applications_stopped']
x=read(A/'comparison/comparison-summary.json')['pairs'][0];assert not x['compatibility_failures']
hot=[5,10,25,33,35,39,43,45,51,55,58,59]
B.mkdir(parents=True,exist_ok=True)
for p in (A/'block').iterdir():
 if p.is_file():shutil.copy2(p,B/p.name)
for name in ['final-runtime-check.json','authorization-and-validation.json','comparison-index.json']:
 shutil.copy2(A/name,B/name)
shutil.copytree(A/'comparison',B/'comparison',dirs_exist_ok=True);shutil.copytree(A/'plots',B/'plots',dirs_exist_ok=True)
file_count=0
for a in b['attempts']:
 d=R/'results'/a['run_id'];o=S/'experiment-records'/a['run_id'];o.mkdir(parents=True,exist_ok=True)
 v=read(d/'evidence-verification.json');assert v['all_files_match'];file_count+=v['file_count']
 for name in ['manifest.json','pipeline-configmap.yaml','runner-status.json','placement-checks.jsonl','preparation-evidence-check.json','evidence-verification.json','outcome-summary.json','lag-summary.json','execution-summary.json','intervention-events.jsonl','resource-history.jsonl','export-status.json']:
  if (d/name).exists():shutil.copy2(d/name,o/name)
 m=read(d/'manifest.json');assert m['config']['SKEW_PARTITION']=='0.2' and m['config']['SKEW_FRACTION']=='0.8'
 if a['preparation_only']:
  assert read(d/'preparation-evidence-check.json')['valid'];body='Empty preparation passed with zero experiment messages. Performance metrics are not applicable.'
 else:
  q=read(d/'outcome-summary.json');assert not q['validity_failures']
  checks=[json.loads(t) for t in (d/'placement-checks.jsonl').read_text().splitlines()];assert {t['stage'] for t in checks}>={'before_production','before_intervention'}
  assert all(t['valid'] and t['reference_sha256']==b['reference_sha256'] for t in checks)
  body=f"Completed {q['completed_by_drain']:,} of {q['admitted_evaluation_cohort']:,} evaluation messages by drain; {q['incomplete_by_drain']:,} unfinished. Conditional completion p99: {q['admitted_cohort_completion_p99_seconds']:.3f} seconds. Message validity checks passed."
 (o/'validation-report.md').write_text(f"# {a['run_id']}\n\nStage: {a['stage']}. Executed commit: `{b['code_commit']}`.\n\n{body}\n\nAll {v['file_count']} evidence files match the original PVC hashes.\n\n[Full comparison and limitations](../8020-comparison-20260913/README.md).\n")
rows=[]
for action in ['none','scale']:
 r=x['runs'][action];m=r['metrics'];o=read(R/'results'/r['run_id']/'outcome-summary.json');parts=o['partition_metrics']['partitions']
 h=[p for p in parts if p['partition'] in hot];c=[p for p in parts if p['partition'] not in hot]
 rows.append(dict(action=action,run_id=r['run_id'],admitted=m['admitted_evaluation_cohort'],completed=m['completed_by_drain'],unfinished=m['incomplete_by_drain'],unfinished_percent=100*m['unfinished_fraction'],completion_p99_seconds=m['admitted_cohort_completion_p99_seconds'],requested_cpu_core_minutes=m['evaluation_and_drain_requested_cpu_seconds']/60,resource_coverage=m['evaluation_and_drain_resource_coverage'],lag_coverage=m['lag_covered_fraction'],selected_traffic_percent=100*sum(p['admitted_messages'] for p in h)/m['admitted_evaluation_cohort'],selected_unfinished=sum(p['incomplete_messages'] for p in h),other_unfinished=sum(p['incomplete_messages'] for p in c),selected_rate_range=[min(p['admitted_per_second'] for p in h),max(p['admitted_per_second'] for p in h)],other_rate_range=[min(p['admitted_per_second'] for p in c),max(p['admitted_per_second'] for p in c)],quality_flags=r['quality_flags'],decision_delay_seconds=r['timing'].get('decision_delay_seconds'),recovery=r['recovery']))
summary=dict(status='complete',code_commit=b['code_commit'],selected_partitions=hot,preparations=4,performance_trials=2,all_evidence_files_match=True,verified_evidence_files=file_count,restoration='verified',all_applications_stopped=True,reference_sha256=b['reference_sha256'],runs=rows,aggregate_backlog_comparison_eligible=all(z['lag_coverage']>=.9 for z in rows),limitations=['One pair, scale first; no independent repetition of this workload comparison.','Starting ownership and original pod placement matched; shared contention and new-pod placement remain uncontrolled.','P99 includes only completed evaluation messages, completion before commit acknowledgment.','99 ms deadline is provisional; cross-node timestamp precision is not independently established.'])
(B/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none'})
fig,axs=plt.subplots(1,3,figsize=(12,4.3),layout='constrained')
for ax,key,title,ylabel in [(axs[0],'unfinished_percent','Unfinished at drain','Percent of evaluation cohort'),(axs[1],'completion_p99_seconds','Completion p99','Seconds; completed cohort only'),(axs[2],'requested_cpu_core_minutes','Requested consumer CPU','Core-minutes; evaluation + drain')]:
 vals=[z[key] for z in rows];bars=ax.bar(['Keep 3','Scale 3 to 6'],vals,color=['#355c7d','#d87532']);ax.bar_label(bars,fmt='%.2f',padding=3);ax.set_ylim(0,max(1,max(vals)*1.2));ax.set_title(title);ax.set_ylabel(ylabel)
fig.suptitle('80% to 12 of 60 partitions: one matched-start comparison\n1,500 messages/s · 5 minutes production',fontsize=13)
fig.savefig(B/'paired-outcomes.png',dpi=180);fig.savefig(B/'paired-outcomes.svg');plt.close(fig)
fig,axs=plt.subplots(2,1,figsize=(10,6),layout='constrained',sharex=True)
for ax,z in zip(axs,rows):
 parts=read(R/'results'/z['run_id']/'outcome-summary.json')['partition_metrics']['partitions'];parts=sorted(parts,key=lambda p:p['partition'])
 ax.bar([p['partition'] for p in parts],[p['admitted_per_second'] for p in parts],color=['#d87532' if p['partition'] in hot else '#355c7d' for p in parts]);ax.set_ylabel('Messages / second');ax.set_title(('Keep 3' if z['action']=='none' else 'Scale 3 to 6')+f": selected 12 received {z['selected_traffic_percent']:.2f}% of evaluation admissions")
for ax in axs: ax.set_ylim(0,135)
axs[-1].set_xlabel('Partition ID');axs[0].legend(handles=[Patch(color='#d87532',label='Selected 12 partitions'),Patch(color='#355c7d',label='Other 48 partitions')],loc='upper right');fig.suptitle('Measured routing confirms the intended 80/20 workload');fig.savefig(B/'partition-arrivals.png',dpi=180);fig.savefig(B/'partition-arrivals.svg');plt.close(fig)
for p in B.glob('*.svg'):p.write_text('\n'.join(s.rstrip() for s in p.read_text().splitlines())+'\n')
text='''# 80/20 keep-three versus scale-to-six comparison — 13 September 2026

Four empty rehearsals and two performance trials completed. Both trials used the same frozen complete starting ownership and original pod identities/machines, verified before production and before the decision. No new node-placement rule was introduced. Original configuration, HPA settings and replica counts were restored and verified; a separate check found all experiment applications stopped.

This workload directs 80% of traffic to **12 of 60 partitions**, not to one partition. The selected IDs (common seed 71) are 5, 10, 25, 33, 35, 39, 43, 45, 51, 55, 58, 59. The remaining traffic goes only to the other 48 partitions. Selection is probabilistic per message; measured shares are reported below.

Three producers targeted 1,500 messages/s total, using a 100-byte payload setting, 2,000 SHA-256 iterations and no artificial sleep. Both trials used 300 seconds production including 60 seconds warm-up, followed by a 120-second drain. Scale ran first; keep-three followed. Scaling was scheduled at production +120 seconds. This first pair retained the preceding single-partition block's rate/work settings; it did not tune the rate after observing results or assume that 80/20 necessarily overloads the consumers.

| Treatment | Evaluation messages | Unfinished at drain | Completion p99 (s) | Requested CPU (core-min) | Lag coverage |
|---|---:|---:|---:|---:|---:|
'''
for z in rows:text+=f"| {'Keep 3' if z['action']=='none' else 'Scale 3 to 6'} | {z['admitted']:,} | {z['unfinished']:,} ({z['unfinished_percent']:.2f}%) | {z['completion_p99_seconds']:.3f} | {z['requested_cpu_core_minutes']:.2f} | {z['lag_coverage']:.2%} |\n"
text+='\nP99 follows distinct acknowledged evaluation messages that completed by the fixed drain cutoff, after application processing and before commit acknowledgment. It is separate from rolling Grafana histogram estimates. Warm-up messages are excluded from this cohort. Requested CPU is integrated over the common 360-second evaluation-through-drain horizon; it is a declaration, not actual CPU consumption or money.\n\n![Paired outcomes](paired-outcomes.png)\n\n![Measured partition arrivals](partition-arrivals.png)\n\n'
text+=f"Keeping three consumers left {rows[0]['unfinished']:,} unfinished evaluation messages; scaling left {rows[1]['unfinished']:,}. Recorded conditional p99 was {rows[0]['completion_p99_seconds']:.3f} versus {rows[1]['completion_p99_seconds']:.3f} seconds. This is one observed comparison, not a general guarantee or an independent replication series.\n\n"
text+='## Measurement limits\n\n'
if not summary['aggregate_backlog_comparison_eligible']:text+='The scaling lag coverage is below the predeclared 90% screen, so this pair is **excluded from aggregate backlog reduction claims**. Per-run plots retain valid observations and gaps as diagnostics. Missing intervals are not zero and are not interpolated across ownership changes. Message outcomes remain evidence-valid.\n\n'
for z in rows:text+=f"- {z['action']}: selected partitions received {z['selected_traffic_percent']:.3f}% of evaluation admissions; individual selected rates ranged {z['selected_rate_range'][0]:.2f}–{z['selected_rate_range'][1]:.2f}/s, others {z['other_rate_range'][0]:.2f}–{z['other_rate_range'][1]:.2f}/s. Selected/other unfinished counts: {z['selected_unfinished']:,}/{z['other_unfinished']:,}. Resource coverage: {z['resource_coverage']:.2%}. Decision delay after schedule: {z['decision_delay_seconds']:.2f} s. Recovery record: {z['recovery'].get('status')}.\n"
text+='\nA recovery-threshold confirmation must not be described as recovery from overload if the run was already below threshold before the decision. The per-run recovery/status and time series are retained. The provisional 99 ms threshold is not used for an SLA success claim; cross-node clock uncertainty remains. New-consumer placement and changing shared-machine load remain uncontrolled. Reassignment and key splitting were not tested.\n\n## Run records\n\n'
for a in b['attempts']:text+=f"- [{a['run_id']}](../{a['run_id']}/validation-report.md): {a['stage']}, {a['status']}.\n"
text+=f"\nExecuted commit: `{b['code_commit']}`. All {file_count} collected evidence files match their PVC source hashes. Cumulative preparation: {b['preparation_seconds_used']:.2f} seconds, within the 1,200-second bound; no retries.\n\n[Exact summary](summary.json), [paired calculations](comparison/comparison-summary.json), [scaling plots](plots/scale/overview.png), [keep-three plots](plots/none/overview.png). Raw events and full exports remain in `results/RUN_ID/`, outside Git.\n"
(B/'README.md').write_text(text)
O=B/'orchestration';O.mkdir(exist_ok=True)
for p in [Path(__file__),A/'backup_evidence.py']+[A.parent/'preliminary-campaign-20260912'/n for n in ['export_plot_data.py','draw_run.py','verify_evidence.py']]:shutil.copy2(p,O/p.name)
print(json.dumps(summary,indent=2))
