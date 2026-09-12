"""Build an auditable campaign inventory from saved run and comparison records."""
import argparse,hashlib,json,time
from pathlib import Path
R=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--complete',action='store_true');args=p.parse_args()
read=lambda p:json.loads(p.read_text())
ledgers=[]
for name in ['sequence-status-v4.json','sequence-status-isolated.json','sequence-status-low-input.json']:
 if (A/name).exists():ledgers.append(read(A/name))
rows=[r for s in ledgers for r in s['trials']];done=[r for r in rows if r['status']=='complete']
lines=['# Nautilus preliminary campaign — 12 September 2026','',
 '**Status: '+('experiment collection complete; check destination and restoration status below.' if args.complete else 'working report; the campaign is still running.')+'**','',
 'This campaign provides preliminary evidence for the simple research questions on partition-level measurement and consumer scaling. It tests scheduled actions with synthetic processing work on shared Nautilus nodes. It does not evaluate the complete adaptive selector, targeted reassignment, key splitting, a tuned HPA, or all rows and repetitions of the planned main study.','',
 f'The retained sequence ledgers currently list {len(done)} completed controlled trials out of {len(rows)} trials started. Prepared but unstarted conditions are listed in the committed protocols and are not counted as completed experiments.','',
 '## Measurement and comparison rules','',
 '- Each run uses a fresh topic, frozen shared configuration, 60-second warm-up and 120-second bounded drain in the controlled comparisons. The original topics and durable event evidence are retained.',
 '- Latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, after application work and before commit acknowledgment. Unfinished messages have no observed completion latency and remain separately reported.',
 '- Rolling 30-second Prometheus rates and histogram quantiles are monitoring views. Final cohort percentiles come from event evidence, not averages of consumer means or rolling p99 values.',
 '- Actual application scraping is every 5 seconds. Historical exports use a 2-second query grid; this does not create independent new scrapes. Lag age combines exporter monotonic observation age and the age of the original Prometheus sample.',
 '- Backlog integration uses only valid contiguous same-owner observations. Missing data and transition gaps are not zero. Backlog aggregates require at least 90% coverage in both runs; common-horizon CPU-request aggregates require at least 95%. Valid message outcomes are retained when monitoring coverage is low.',
 '- Requested consumer CPU is integrated over the same evaluation-through-drain horizon in both conditions. It is not measured whole-cluster CPU, monetary cost, or resource use during preparation.',
 '- The 99 ms threshold remains provisional; recorded clock probes do not establish a sufficiently tight independent cross-node bound for that SLA. No deadline-based success claim is made.',
 '- Each family has matched workload seeds and opposite action orders across its two pairs. Initial assignments and node placements are recorded, not forced identical. Each metric reports its own eligible pair count. Two pairs provide descriptive repetition, not a strong confidence interval or a general causal/superiority claim.','',
 '## Controlled run inventory','',
 '| Family / pair | Action | Run | Evaluation admissions | Unfinished | Conditional completion p99 (s) | Lag coverage |','|---|---|---|---:|---:|---:|---:|']
for row in done:
 d=R/'results'/row['run_id'];o=read(d/'outcome-summary.json');lag=read(d/'lag-summary.json')
 lines.append(f"| {row['family']} / {row['pair']} | {row['action']} | [{row['run_id']}](../{row['run_id']}/validation-report.md) | {o['admitted_evaluation_cohort']:,} | {o['incomplete_by_drain']:,} | {o['admitted_cohort_completion_p99_seconds']:.3f} | {100*lag['covered_fraction']:.2f}% |")
lines += ['', '## Per-family paired results','']
for family in ['sustained','short','isolated','low-input']:
 d=A/'comparisons'/family
 if not (d/'comparison-summary.json').exists():continue
 s=read(d/'comparison-summary.json');index=read(d/'index.json')
 lines += [f"### {family}",'',f"Status: {index['status']}. [Complete tables, timing and limits](comparisons/{family}/comparison-report.md).",'']
 for pair in s['pairs']:
  n,c=pair['runs']['none'],pair['runs']['scale'];nm,cm=n['metrics'],c['metrics']
  cpu=lambda r:r['evaluation_and_drain_requested_cpu_seconds']/60
  lines += [f"Pair {pair['pair']} (seed {n['workload_seed']}): unfinished {100*nm['unfinished_fraction']:.3f}% → {100*cm['unfinished_fraction']:.3f}%; completed-cohort p99 {nm['admitted_cohort_completion_p99_seconds']:.3f} → {cm['admitted_cohort_completion_p99_seconds']:.3f} s; requested consumer CPU {cpu(nm):.3f} → {cpu(cm):.3f} core-minutes. The arrow compares keep-3 with schedule-3-to-6."]
  for action,row in pair['runs'].items():
   if row['quality_flags']:lines.append(action+' limitations: '+'; '.join(row['quality_flags'])+'.')
  lines.append('')
 lines += [f'![{family} paired outcomes](comparisons/{family}/paired-outcomes.png)','']
lines += ['## Calibration and retained technical attempts','',
 '| Run | Role in the campaign |','|---|---|',
 '| [run-20260912-031020](../run-20260912-031020/validation-report.md) | Six-consumer, 12,000/s, no-added-work calibration; no intended sustained backlog. Existing HPA active, six replicas observed. |',
 '| [run-20260912-042303](../run-20260912-042303/validation-report.md) | Excluded: HPA restored six after three were requested; interrupted trial and cleanup diagnostic. |',
 '| [run-20260912-043547](../run-20260912-043547/validation-report.md) | Three-consumer no-added-work calibration; message outcomes retained, resource comparison excluded after Mac sleep caused poor coverage. Historical Prometheus export recovered with original failure status retained. |',
 '| [run-20260912-052133](../run-20260912-052133/validation-report.md) | Excluded: group-generation commit errors prevented readiness; no production released. |',
 '| [run-20260912-053411](../run-20260912-053411/validation-report.md) | Limited pressure diagnostic; message outcomes retained, mixed-field monitoring coverage below the qualifying threshold. |',
 '| [run-20260912-054927](../run-20260912-054927/validation-report.md) | Qualified pressure calibration: 180,000 evaluation admissions, 3,103 unfinished, fixed ownership and 98.33% lag coverage. |',
 '| [run-20260912-083417](../run-20260912-083417/validation-report.md) | Qualified dedicated-partition calibration: one partition and consumer, 1,200 admissions/s, 639.183 unique completions per evaluation second, 24,687 of 144,000 admissions unfinished by drain, fixed ownership and 98.33% lag coverage. |',
 '| [run-20260912-060649](../run-20260912-060649/validation-report.md) | Excluded interrupted scale attempt: the legacy freshness guard compared wall clocks across machines. |',
 '| [run-20260912-062803](../run-20260912-062803/validation-report.md) | Excluded interrupted attempt: initial PromQL union omitted required original-sample timestamps; stopped before the scale action. |',
 '| [run-20260912-063614](../run-20260912-063614/validation-report.md) | Excluded pre-production diagnostic: an overly strict readiness check rejected unresolved positions on an empty topic. No production released. |','',
 '[Balanced-input calibration figure and exact consumer counts](calibration-diagnostic/calibration-diagnostic.json) show that balanced arrivals can coexist with uneven processing progress. They do not isolate hardware differences, contention or another cause on shared nodes.','',
 '[Dedicated-partition qualification](single-partition-calibration/qualification.json) records all predeclared checks. Median backlog in the three boundary windows grew from 37,580 to 65,910 to 98,837 offsets. This establishes overload for that consumer, node and interval, not a universal service-capacity constant. Later concentrated-input runs may have different owners, placements and additional cold-partition traffic.','',
 '## Relationship to the proposal experiment table','',
 '| Table 1 condition | What this campaign can support |','|---|---|',
 '| A — balanced demand with sufficient capacity | Prepared lower-input action comparisons; claim headroom only if their measured no-action outcomes confirm it. |',
 '| B — balanced sustained pressure | Controlled CPU-workload no-action/scaling pairs with common follow-up and separate resource accounting. |',
 '| C — busy partitions share a consumer with spare capacity elsewhere | Targeted reassignment is not implemented or tested here. |',
 '| D — one overloaded sequential partition | A dedicated one-partition/one-consumer calibration qualified before the concentrated-input comparison. Completed controlled contrasts, if listed above, must be interpreted with their recorded hot-owner placement and cold traffic. |',
 '| E — short burst returning to ordinary demand | The short core workload ends production at zero input; it is an episode-duration pilot, not the full return-to-normal burst condition. |',
 '| F — moving hotspots | Not tested in this campaign. |',
 '| G — equal arrivals with differing record costs | All controlled records use the same synthetic CPU task. Uneven node progress is not the planned variable-record-cost experiment. |','',
 '## Evidence locations and reproducibility','',
 'Full local event evidence is under `results/RUN_ID/` in the active repository; original closed-run event files are retained on the Nautilus producer and consumer PVCs. `evidence-verification.json` compares local bytes with PVC SHA-256 values. Completed validated evidence may be stored as lossless gzip with a round-trip hash record. Git contains smaller configurations, outcome and monitoring summaries, plots, source identities and file manifests; it is not the full raw-data backup.','',
 'The paired source signatures compare the producer/consumer application files, Python and package versions that actually ran. Baseline run-20260912-070155 launched with HPA wrapper/docs/tests changes in the working tree, later committed as `6644c86`; the launch status is retained and the later commit is not claimed as its launch revision. Its direct scheduled path and application signatures match the paired run.','',
 'Local backups include a verified full Git bundle through `b42bbcf` and an incremental bundle through `f2e36f3`. Final commit, push, restoration and online proposal status are recorded separately at campaign completion. Do not treat a local commit as a successful GitHub push or a local LaTeX compile as an online Overleaf update.','']
(A/'campaign-report.md').write_text('\n'.join(lines))
index=dict(generated_epoch=time.time(),status='complete_requested' if args.complete else 'working',completed_controlled_trials=len(done),started_controlled_trials=len(rows),runs=rows,ledgers=[dict(path=name,sha256=hashlib.sha256((A/name).read_bytes()).hexdigest()) for name in ['sequence-status-v4.json','sequence-status-isolated.json','sequence-status-low-input.json'] if (A/name).exists()])
(A/'campaign-evidence-index.json').write_text(json.dumps(index,indent=2)+'\n')
print(A/'campaign-report.md')
