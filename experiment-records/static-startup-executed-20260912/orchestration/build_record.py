from pathlib import Path
import json,shutil,hashlib,math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
A=Path(__file__).resolve().parent; R=Path('/Users/soheila/Desktop/Thesis-26-27/code'); S=A/'staged'; S.mkdir(exist_ok=True)
read=lambda p:json.loads(p.read_text())
b=read(A/'block/block-status.json'); assert b['status']=='complete' and b['restoration']=='verified'
assert len(b['attempts'])==6 and len(b['runs'])==2
for a in b['attempts'][:4]: assert a['status']=='preparation_verified'
x=read(A/'comparison/comparison-summary.json')['pairs'][0];assert not x['compatibility_failures']
B=S/'experiment-records/static-startup-executed-20260912';B.mkdir(parents=True,exist_ok=True)
for p in (A/'block').iterdir():
 if p.is_file():shutil.copy2(p,B/p.name)
for n in ['final-runtime-check.json','authorization.json','reporting-source-hashes.json','source-sync.log']:
 if (A/n).exists():shutil.copy2(A/n,B/n)
assert read(A/'final-runtime-check.json')['all_applications_stopped']
shutil.copytree(A/'comparison',B/'comparison',dirs_exist_ok=True)
shutil.copytree(A/'plots',B/'plots',dirs_exist_ok=True)
files=['manifest.json','pipeline-configmap.yaml','runner-status.json','placement-checks.jsonl','preparation-evidence-check.json','evidence-verification.json','outcome-summary.json','lag-summary.json','execution-summary.json','intervention-events.jsonl','resource-history.jsonl','export-status.json']
counts=0
for a in b['attempts']:
 d=R/'results'/a['run_id'];o=S/'experiment-records'/a['run_id'];o.mkdir(parents=True,exist_ok=True)
 v=read(d/'evidence-verification.json');assert v['all_files_match'];counts+=v['file_count']
 for f in files:
  if (d/f).exists(): shutil.copy2(d/f,o/f)
 if a['preparation_only']:
  assert read(d/'preparation-evidence-check.json')['valid']
  body='Empty preparation passed. No workload was released; zero-message evidence passed. Performance metrics are not applicable.'
 else:
  q=read(d/'outcome-summary.json');assert not q['validity_failures']
  checks=[json.loads(t) for t in (d/'placement-checks.jsonl').read_text().splitlines()]
  assert {t['stage'] for t in checks} >= {'before_production','before_intervention'}
  assert all(t['valid'] and t['reference_sha256']==b['reference_sha256'] for t in checks)
  body=f"Completed {q['completed_by_drain']:,} of {q['admitted_evaluation_cohort']:,} evaluation admissions by drain; {q['incomplete_by_drain']:,} unfinished. Conditional completion p99: {q['admitted_cohort_completion_p99_seconds']:.3f} seconds. Outcome validity checks passed."
 (o/'validation-report.md').write_text(f"# {a['run_id']}\n\nStage: {a['stage']}. Executed source commit: `{b['code_commit']}`.\n\n{body}\n\nOriginal PVC evidence matches all {v['file_count']} local evidence files by SHA-256.\n\n[Full block report and limitations](../static-startup-executed-20260912/README.md).\n")
none=x['runs']['none']['metrics'];scale=x['runs']['scale']['metrics']
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none'})
fig,axs=plt.subplots(1,3,figsize=(12,4.3),layout='constrained')
for ax,key,factor,title,ylabel in [(axs[0],'unfinished_fraction',100,'Unfinished at drain','Percent of evaluation cohort'),(axs[1],'admitted_cohort_completion_p99_seconds',1,'Completion p99','Seconds; completed cohort only'),(axs[2],'evaluation_and_drain_requested_cpu_seconds',1/60,'Requested consumer CPU','Core-minutes; evaluation + drain')]:
 vals=[none[key]*factor,scale[key]*factor];bars=ax.bar(['Keep 3','Scale 3 to 6'],vals,color=['#355c7d','#d87532']);ax.bar_label(bars,fmt='%.2f',padding=3);ax.set_ylim(0,max(vals)*1.2);ax.set_title(title);ax.set_ylabel(ylabel)
fig.suptitle('Concentrated input: one matched-start comparison\n80% to partition 0 · 1,500 messages/s · 5 minutes production',fontsize=13)
fig.savefig(B/'paired-outcomes.png',dpi=180);fig.savefig(B/'paired-outcomes.svg');plt.close(fig)
fig,ax=plt.subplots(figsize=(9,4.7),layout='constrained')
for action,color in [('none','#355c7d'),('scale','#d87532')]:
 rid=x['runs'][action]['run_id'];d=R/'results'/rid;m=read(d/'manifest.json');lag=read(d/'lag-summary.json');xx=[];yy=[];prev=None
 for row in lag['snapshots']:
  if prev and row['valid'] and prev['valid'] and row.get('growth_offsets_per_second') is None:xx.append(math.nan);yy.append(math.nan)
  xx.append(row['timestamp']-m['evaluation_start_epoch']);yy.append(row.get('processing_backlog',math.nan) if row['valid'] else math.nan);prev=row
 ax.plot(xx,yy,label=('Keep 3' if action=='none' else 'Scale 3 to 6')+f" ({lag['covered_fraction']:.1%} coverage)",color=color)
ax.axvline(60,ls='--',color='#555',label='Scheduled decision');ax.set(xlim=(0,240),ylim=(0,None),xlabel='Seconds from evaluation start',ylabel='Completion-frontier backlog (offsets)',title='Backlog persisted in both runs\nMissing or discontinuous observations remain gaps');ax.legend();fig.savefig(B/'backlog-comparison.png',dpi=180);fig.savefig(B/'backlog-comparison.svg');plt.close(fig)
rows=[]
for action in ['none','scale']:
 r=x['runs'][action];m=r['metrics'];d=R/'results'/r['run_id'];o=read(d/'outcome-summary.json');hot=next(t for t in o['partition_metrics']['partitions'] if t['partition']==0)
 rows.append({'action':action,'run_id':r['run_id'],'admitted':m['admitted_evaluation_cohort'],'completed':m['completed_by_drain'],'unfinished':m['incomplete_by_drain'],'unfinished_percent':100*m['unfinished_fraction'],'completion_p99_seconds':m['admitted_cohort_completion_p99_seconds'],'requested_cpu_core_minutes':m['evaluation_and_drain_requested_cpu_seconds']/60,'resource_coverage':m['evaluation_and_drain_resource_coverage'],'lag_coverage':m['lag_covered_fraction'],'partition0_unfinished':hot['incomplete_messages'],'other_partitions_unfinished':m['incomplete_by_drain']-hot['incomplete_messages'],'partition0_arrivals_per_second':hot['admitted_per_second'],'partition0_completions_per_second':hot['unique_completions_per_second']})
summary={'status':'complete','code_commit':b['code_commit'],'preparations':4,'performance_trials':2,'all_evidence_files_match':True,'verified_evidence_files':counts,'restoration':'verified','all_applications_stopped':True,'reference_sha256':b['reference_sha256'],'runs':rows,'limitations':['One pair, scale first; no independent repetition of this revised setup.','Shared-machine contention and new-consumer placement remain uncontrolled.','Static membership differs from the earlier campaign; do not pool as identical treatment.','Completion is before commit acknowledgment; p99 excludes unfinished messages.','99 ms threshold remains provisional; cross-node clock precision is not established.']}
(B/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
table='| Treatment | Evaluation messages | Unfinished | Completion p99 (s) | Requested CPU (core-min) | Lag coverage |\n|---|---:|---:|---:|---:|---:|\n'
for z in rows:table+=f"| {'Keep 3' if z['action']=='none' else 'Scale 3 to 6'} | {z['admitted']:,} | {z['unfinished']:,} ({z['unfinished_percent']:.2f}%) | {z['completion_p99_seconds']:.2f} | {z['requested_cpu_core_minutes']:.2f} | {z['lag_coverage']:.2%} |\n"
text='''# Controlled concentrated-input repeat — 12 September 2026

Four empty preparations and both performance trials completed. Original shared configuration, HPA identity/settings and replica counts were restored and verified. A separate check found all nine experiment applications stopped. Consumer source was synchronized for this block; restoration of configuration does not revert that source deployment.

This is one new comparison, separate from the earlier 16 trials. Both treatments used the same static-membership setup and workload seed 71. Three producers targeted 1,500 messages/s total, 80% to partition 0, 60 partitions, 100-byte payload setting, 2,000 SHA-256 iterations and no artificial sleep. Production lasted 300 seconds including 60 seconds warm-up, followed by 120 seconds drain. The order was scale then keep-three, as approved before execution.

'''+table+'''
The evaluation-to-drain resource horizon is 360 seconds, with 100% resource coverage in both runs. Requested CPU measures container declarations, not actual CPU consumption or monetary cost. P99 covers only completed distinct acknowledged evaluation messages, with completion before commit acknowledgment. The provisional 99 ms threshold is retained in raw summaries but is not used for an SLA success claim.

![Paired outcomes](paired-outcomes.png)

![Backlog comparison](backlog-comparison.png)

The observed unfinished fraction and conditional p99 were lower in the scaling trial, with greater requested CPU. Backlog continued growing in valid observations after scaling; neither run confirmed the predeclared recovery condition (1,500 offsets or less for 20 seconds before production ended). The drain is a separate period. Lag coverage meets the 90% aggregate screen, but missing transition intervals remain missing.

## What was controlled and what was not

The first preparation captured the actual complete 60-partition assignment, then froze it. The next three empty preparations checked restart with three consumers, six consumers, and return to three. All four passed zero-message audits. Both real trials independently passed the same ownership and original-pod identity checks before production and before the scheduled decision. No new node-placement constraint was added.

Partition 0 started on Consumer 0 / patternlab in both trials. Consumer 1 stayed on k8s-gpu-01 and Consumer 2 on unseenu in the frozen starting group. During scaling, recorded assignment events moved partition 0 from Consumer 0 to Consumer 5 and then Consumer 4 (k8s-usra-01). The added-consumer placement was not fixed in advance. Event timestamps on different machines do not establish precise millisecond handover durations.

The scaling decision was recorded about 8.86 seconds after the scheduled point; the keep decision about 7.65 seconds afterward. These measured delays are retained; the action did not occur exactly at production +120 seconds. The backlog plot marks the scheduled point.

This addresses the earlier starting-owner/machine mismatch for this pair. It does not remove changing shared-machine contention or isolate replica count from the resulting reassignment to another machine. There is only one pair in one order, so it is descriptive preliminary evidence, not a general causal estimate or proof that another mitigation would work better. Static membership also distinguishes this block from the earlier dynamic-membership campaign.

## Run records

'''
for a in b['attempts']: text+=f"- [{a['run_id']}](../{a['run_id']}/validation-report.md): {a['stage']}, {a['status']}.\n"
text+='\n## Partition outcomes\n\n'
for z in rows:text+=f"- {z['action']}: partition 0 had {z['partition0_unfinished']:,} unfinished evaluation messages; the other 59 partitions had {z['other_partitions_unfinished']:,}. Partition-0 evaluation admissions averaged {z['partition0_arrivals_per_second']:.2f}/s and distinct completions during evaluation {z['partition0_completions_per_second']:.2f}/s.\n"
text+='\nCompletion rates above include completions of earlier admissions; unfinished counts follow the evaluation-admission cohort. They are different populations.\n\n## Evidence and provenance\n\n'
text+=f"Executed commit: `{b['code_commit']}`. All {counts} collected producer/consumer evidence files matched their original PVC hashes. Cumulative preparation: {b['preparation_seconds_used']:.2f} seconds, within the 1,200-second bound. No retry or reference replacement occurred.\n\n"
text+='[Machine-readable summary](summary.json), [paired calculations](comparison/comparison-summary.json), [scaling plots](plots/scale/overview.png), [keep-three plots](plots/none/overview.png). Full raw data stays in `results/RUN_ID/` and outside Git; these summaries are not a raw-data backup.\n'
(B/'README.md').write_text(text)
# Keep the user-facing page short.
p=R/'EXPERIMENT_REGISTER.md';old=p.read_text();(A/'register-before.md').write_text(old)
new=old.replace('**16 comparison runs completed. The next experiment is waiting for your confirmation.**','**18 comparison runs completed. The latest controlled repeat finished successfully.**')
new=new.replace('For each condition below, we ran twice with three consumers and twice with scaling from three to six: four runs per condition.','The first four conditions each had four runs: twice with three consumers and twice with scaling from three to six. The latest repeat added two runs.')
start=new.index('**What is next?**')
new=new[:start]+'''**Latest controlled repeat — 5 minutes**

Both runs started with the same partition ownership and original pods/machines. Keeping three consumers left **39.49% unfinished**, versus **25.10% after scaling**. Recorded completion p99 was **228.44 versus 192.79 seconds**. Scaling used more requested CPU and backlog still grew. This is one comparison; shared-machine conditions remain a limitation.

All four empty startup rehearsals passed. Both real runs finished, evidence checks passed, and original settings were restored.

**What is next?**

Review this new result before choosing another experiment. No additional run has started.

[Latest result and plots](experiment-records/static-startup-executed-20260912/README.md) · [Earlier detailed results](experiment-records/README.md)
'''
(S/'EXPERIMENT_REGISTER.md').write_text(new)
# Link the new dated record without changing historical results.
rel='experiment-records/README.md';old=(R/rel).read_text();(A/'records-readme-before.md').write_text(old)
(S/rel).write_text(old+'\nThe [controlled repeat](static-startup-executed-20260912/README.md) adds four successful empty preparations and two completed performance trials with a common frozen starting reference.\n')
print(json.dumps(summary,indent=2))
