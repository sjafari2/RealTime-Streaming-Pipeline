# Experiment register and progress

Last reviewed: 12 September 2026. This is the main entry point for following the experiments. It is a reviewed record, not a live cluster dashboard. Run IDs are retained exactly as recorded; consult each manifest/report for its timestamps and time zone.

## Current progress

- **16 completed comparison trials:** four conditions, each with two workload-seed pairs (keep 3 consumers versus scale 3 to 6).
- **2 completed initial collection pilots:** balanced and 80/20 skewed traffic.
- **5 calibration/pressure diagnostics with retained outcomes:** qualifications differ; see below.
- **5 excluded technical attempts:** some were interrupted after traffic started; others never released production. They are not comparison results.
- **6 later empty preparations rejected:** ownership did not match; zero experiment messages and no new performance pair.
- **Next block is prepared and awaiting user confirmation:** four empty startup checks, then only if approved and all checks pass, two single-partition stress trials. It has not been deployed or run.

These categories cover 34 recorded run IDs (28 individual Git record folders plus six preparations in the follow-up block). A completed trial does not mean all its messages finished by the cutoff. A technical exclusion is retained, not erased.

## What the completed comparisons show

All four conditions below have four trials: two keep-three and two scheduled scale-to-six. Production durations include one minute of warm-up; each then has two minutes of drain. Preparation and collection are additional time.

| Condition | Production per trial | Observed result | Reports and saved plots |
|---|---:|---|---|
| Balanced sustained pressure | 10 minutes | Keep-three left 9.62–11.38% unfinished; scale trials finished their evaluation cohorts by drain, with higher requested CPU. | [Report](experiment-records/campaign-20260912/comparisons/sustained/comparison-report.md), [plot](experiment-records/campaign-20260912/comparisons/sustained/paired-outcomes.png) |
| Short balanced pressure | 3 minutes | Scaling reduced observed completion p99. One baseline left 1.245% unfinished; the other and both scale trials left zero. Scale lag coverage was insufficient for aggregate backlog comparisons. | [Report](experiment-records/campaign-20260912/comparisons/short/comparison-report.md), [plot](experiment-records/campaign-20260912/comparisons/short/paired-outcomes.png) |
| Concentrated input: 80% to partition 0 | 5 minutes | Backlog persisted after scaling; scale trials left 44.40–45.71% unfinished. Different initial hot-partition owners/machines confound the treatment comparison. | [Report](experiment-records/campaign-20260912/comparisons/isolated/comparison-report.md), [plot](experiment-records/campaign-20260912/comparisons/isolated/paired-outcomes.png) |
| Low balanced input | 3 minutes | All four cohorts finished. Scaling increased requested CPU without improving the observed p99. | [Report](experiment-records/campaign-20260912/comparisons/low-input/comparison-report.md), [plot](experiment-records/campaign-20260912/comparisons/low-input/paired-outcomes.png) |

These are preliminary observations on shared machines, with two pairs per condition. They do not establish a universal scaling effect or validate the planned adaptive selector. The short-pressure condition stops input; it is not the full burst-returning-to-normal experiment.

## Every completed comparison trial

Completion p99 below is in **seconds**, among distinct acknowledged evaluation messages completed by the common drain cutoff. Read it with unfinished counts. It is not rolling Grafana p99. Lag coverage applies to monitoring, not the completeness of the message cohort.

| Family / pair | Action | Run | Evaluation admissions | Unfinished | Conditional completion p99 (s) | Lag coverage |
|---|---|---|---:|---:|---:|---:|
| sustained / L1 | scale | [run-20260912-064358](experiment-records/run-20260912-064358/validation-report.md) | 810,000 | 0 | 95.580 | 93.70% |
| sustained / L1 | none | [run-20260912-070155](experiment-records/run-20260912-070155/validation-report.md) | 810,000 | 92,148 | 292.424 | 99.63% |
| short / S1 | none | [run-20260912-071918](experiment-records/run-20260912-071918/validation-report.md) | 180,000 | 2,241 | 121.822 | 98.33% |
| short / S1 | scale | [run-20260912-072753](experiment-records/run-20260912-072753/validation-report.md) | 180,000 | 0 | 75.918 | 65.00% |
| sustained / L2 | none | [run-20260912-073652](experiment-records/run-20260912-073652/validation-report.md) | 810,000 | 77,946 | 266.316 | 99.63% |
| sustained / L2 | scale | [run-20260912-075420](experiment-records/run-20260912-075420/validation-report.md) | 810,000 | 0 | 79.096 | 95.93% |
| short / S2 | scale | [run-20260912-081218](experiment-records/run-20260912-081218/validation-report.md) | 180,000 | 0 | 62.936 | 80.00% |
| short / S2 | none | [run-20260912-082113](experiment-records/run-20260912-082113/validation-report.md) | 180,000 | 0 | 115.817 | 98.33% |
| isolated / D1 | none | [run-20260912-085711](experiment-records/run-20260912-085711/validation-report.md) | 360,000 | 185,039 | 259.655 | 99.17% |
| isolated / D1 | scale | [run-20260912-090802](experiment-records/run-20260912-090802/validation-report.md) | 359,999 | 159,847 | 246.733 | 89.17% |
| isolated / D2 | scale | [run-20260912-091909](experiment-records/run-20260912-091909/validation-report.md) | 360,000 | 164,572 | 250.328 | 94.17% |
| isolated / D2 | none | [run-20260912-093023](experiment-records/run-20260912-093023/validation-report.md) | 360,000 | 191,127 | 268.532 | 99.17% |
| low-input / A1 | none | [run-20260912-094344](experiment-records/run-20260912-094344/validation-report.md) | 72,000 | 0 | 0.824 | 98.33% |
| low-input / A1 | scale | [run-20260912-095159](experiment-records/run-20260912-095159/validation-report.md) | 72,000 | 0 | 0.939 | 91.67% |
| low-input / A2 | scale | [run-20260912-100035](experiment-records/run-20260912-100035/validation-report.md) | 72,000 | 0 | 0.900 | 93.33% |
| low-input / A2 | none | [run-20260912-100910](experiment-records/run-20260912-100910/validation-report.md) | 72,000 | 0 | 0.825 | 98.33% |

## Initial collection pilots

Both had 3 producers, 6 consumers, 60 partitions, a 9,000/s total target, 5 minutes production, no warm-up and 1 minute drain. There was no scheduled mitigation. These were collection checks, not matched scaling comparisons.

| Run | Input | Evaluation admissions | Unfinished | Recorded completion p99 |
|---|---|---:|---:|---:|
| [run-20260911-214924](experiment-records/run-20260911-214924/validation-report.md) | 80% aimed at 12 of 60 partitions | 2,699,197 | 0 | 662.856 ms |
| [run-20260911-234758](experiment-records/run-20260911-234758/validation-report.md) | Balanced | 2,699,808 | 0 | 764.188 ms |

Their clock/monitoring qualifications remain in the linked reports. The 80%-to-12-partitions pilot is a different workload from the later 80%-to-partition-0 stress trials.

## Calibrations, diagnostics and technical exclusions

| Run | Role in the campaign |
|---|---|
| [run-20260912-031020](experiment-records/run-20260912-031020/validation-report.md) | Six-consumer, 12,000/s, no-added-work calibration; no intended sustained backlog. Existing HPA active, six replicas observed. |
| [run-20260912-042303](experiment-records/run-20260912-042303/validation-report.md) | Excluded: HPA restored six after three were requested; interrupted trial and cleanup diagnostic. |
| [run-20260912-043547](experiment-records/run-20260912-043547/validation-report.md) | Three-consumer no-added-work calibration; message outcomes retained, resource comparison excluded after Mac sleep caused poor coverage. Historical Prometheus export recovered with original failure status retained. |
| [run-20260912-052133](experiment-records/run-20260912-052133/validation-report.md) | Excluded: group-generation commit errors prevented readiness; no production released. |
| [run-20260912-053411](experiment-records/run-20260912-053411/validation-report.md) | Limited pressure diagnostic; message outcomes retained, mixed-field monitoring coverage below the qualifying threshold. |
| [run-20260912-054927](experiment-records/run-20260912-054927/validation-report.md) | Qualified pressure calibration: 180,000 evaluation admissions, 3,103 unfinished, fixed ownership and 98.33% lag coverage. |
| [run-20260912-083417](experiment-records/run-20260912-083417/validation-report.md) | Qualified dedicated-partition calibration: one partition and consumer, 1,200 admissions/s, 639.183 unique completions per evaluation second, 24,687 of 144,000 admissions unfinished by drain, fixed ownership and 98.33% lag coverage. |
| [run-20260912-060649](experiment-records/run-20260912-060649/validation-report.md) | Excluded interrupted scale attempt: the legacy freshness guard compared wall clocks across machines. |
| [run-20260912-062803](experiment-records/run-20260912-062803/validation-report.md) | Excluded interrupted attempt: initial PromQL union omitted required original-sample timestamps; stopped before the scale action. |
| [run-20260912-063614](experiment-records/run-20260912-063614/validation-report.md) | Excluded pre-production diagnostic: an overly strict readiness check rejected unresolved positions on an empty topic. No production released. |

## Rejected empty preparations

[Full ownership and restoration record](experiment-records/controlled-followup-20260912/README.md).

| Run | Status | Experiment messages |
|---|---|---:|
| run-20260912-170614 | Rejected: ownership mismatch | 0 |
| run-20260912-170822 | Rejected: ownership mismatch | 0 |
| run-20260912-171032 | Rejected: ownership mismatch | 0 |
| run-20260912-171242 | Rejected: ownership mismatch | 0 |
| run-20260912-171450 | Rejected: ownership mismatch | 0 |
| run-20260912-171702 | Rejected: ownership mismatch | 0 |

The six-attempt bound stopped that block. No scale/keep performance comparison was obtained. Original settings were restored and verified in its saved record. Three earlier launch failures occurred during initial inspection before a run/preparation started; the block report retains them separately without assigning invented run IDs.

## Next work and approval status

1. **Awaiting confirmation:** [revised static-member startup and concentrated-input pair](experiments/controlled-followup-20260912/STATIC_STARTUP.md). Four empty checks (3, 3, 6, then 3 consumers); if all pass and the comparison is approved, scale then keep-three at 1,500/s total with 80% aimed at partition 0. Stop on first failure. The 145 local tests do not constitute live validation.
2. **After an accepted pair:** reconcile messages, assess coverage, save paired plots, report placement and limitations, then update this register and the proposal results.
3. **Still untested main-study work:** targeted reassignment, the full burst-return-to-normal condition, moving hotspots, variable per-record costs, and the complete adaptive policy. See the [proposal-table mapping](experiment-records/campaign-20260912/campaign-report.md).

## Where the files are kept

- [Complete campaign report](experiment-records/campaign-20260912/campaign-report.md): all paired findings and limitations.
- [Versioned records](experiment-records/README.md): frozen settings, compact summaries, plots and provenance.
- Full local message evidence: `results/RUN_ID/` under this repository. These large files are ignored by Git. Original closed-run evidence was retained on Nautilus PVCs; this register does not perform a fresh storage check.
- Git history preserves reviewed record changes. A local Git commit is not a remote backup. No Google Cloud data backup is configured.
- [Active proposal](https://www.overleaf.com/project/6aa3337d078205869d3c5fe5).

## Keeping this register current

After each run or bounded block, add its actual run ID, status, configuration/report/plot links, message outcomes and metric-specific limitations. Update the next-step status and last-reviewed date. Preserve rejected and interrupted attempts. Commit the reviewed record with the result changes, and report whether the push succeeded. Never mark a planned run completed or replace an earlier run's source revision with today's commit.

This register is updated as part of experiment work; it does not automatically refresh or launch experiments. The frozen run records remain the underlying evidence.
