# Preliminary action comparison

Status: **partial_1_of_2_predeclared_pairs**. Each pair below uses its own matched workload seed. Short, sustained, concentrated-partition and low-input families are analyzed separately.

## Pair D1 — seed 41

Keep three consumers: `run-20260912-085711`. Schedule three to six: `run-20260912-090802`.

| Measure | Keep 3 | Schedule 3 → 6 |
|---|---:|---:|
| Distinct evaluation admissions | 360,000 | 359,999 |
| Unfinished at drain | 185,039 | 159,847 |
| Unfinished fraction (%) | 51.400 | 44.402 |
| Recorded completed-cohort mean (s) | 132.444 | 111.432 |
| Recorded completed-cohort p99 (s) | 259.655 | 246.733 |
| Admissions / evaluation second | 1,500.000 | 1,499.996 |
| Unique in-evaluation completions / second | 690.212 | 769.371 |
| Covered-interval mean processing backlog (offsets) | 145,890.4 | 128,736.8 |
| Lag coverage (%) | 99.17 | 89.17 |
| Requested consumer CPU (core-minutes, evaluation + drain) | 36.000 | 65.779 |
| Resource coverage for that horizon (%) | 100.00 | 100.00 |

Compatibility checks: passed (workload configuration, action settings and recorded application/Python/package signatures). Both full message-outcome evidence checks passed: True.
Keep 3 quality flags: none at the predeclared coverage screens. Recovery: `{"censored": true, "followup_seconds": 180.0, "hold_seconds": 20, "seconds_to_confirmation": null, "status": "not_observed_by_evaluation_end", "threshold_offsets": 1500}`.
Scale quality flags: Lag coverage below 90%; transition gaps remain part of the result. Recovery: `{"censored": true, "followup_seconds": 180.0, "hold_seconds": 20, "seconds_to_confirmation": null, "status": "not_observed_by_evaluation_end", "threshold_offsets": 1500}`.

| New consumer | Node | Request → Kubernetes Ready (s) | Request → first recorded completion (s) |
|---|---|---:|---:|
| consumer-sts-3 | k8s-u200-00.calit2.optiputer.net | 9.84 | 12.94 |
| consumer-sts-4 | k8s-chase-ci-10.calit2.optiputer.net | 14.84 | 16.00 |
| consumer-sts-5 | k8s-ravi-01.calit2.optiputer.net | 12.84 | 15.95 |

| Condition | Initial partition-0 owner | Initial owner node | Partition-0 admissions / s | Partition-0 completions / evaluation s | Partition-0 unfinished | Other-partition unfinished |
|---|---|---|---:|---:|---:|---:|
| Keep 3 | consumer-sts-1 | k8s-gpu-01.calit2.optiputer.net | 1200.067 | 451.375 | 171,754 | 13,285 |
| Scale | consumer-sts-0 | patternlab.calit2.optiputer.net | 1200.075 | 486.762 | 155,189 | 4,658 |

The paired workload seed fixes the generated routing distribution, not the initial Kafka assignment or node capacity. A hot-owner change can also change available processing capacity. The separate one-partition calibration is one recorded-node check, not a capacity constant valid for every node or time.

![Pair D1 partition detail](pair-D1-partition-0.png)

| Condition | Observed partition-0 owner / process prefix | Recorded node(s) | First valid evaluation second | Last valid evaluation second |
|---|---|---|---:|---:|
| none | `consumer-sts-1/30130f64` | k8s-gpu-01.calit2.optiputer.net | 2.00 | 240.00 |
| scale | `consumer-sts-0/3a93624b` | patternlab.calit2.optiputer.net | 2.00 | 76.00 |
| scale | `consumer-sts-5/eb6d5f30` | k8s-ravi-01.calit2.optiputer.net | 100.00 | 240.00 |

These intervals locate valid partition-owner observations, not exact handover times; invalid gaps break intervals even if the same owner returns. Node names come from resource observations within the displayed interval plus a 15-second sampling margin. Full process identities and source hashes are in the partition plot data JSON. Multiple recorded nodes would be shown explicitly.


![Pair D1 monitoring curves](pair-D1-timecourse.png)

## Descriptive differences across matched seeds

Each difference is scale minus keep-3 for one matched seed. Means and sample standard deviations below summarize these paired run-level differences; they do not pool message latencies. Coverage screens apply separately to each metric.

| Measure | Eligible pairs | Mean paired difference | Minimum | Maximum | Sample SD |
|---|---:|---:|---:|---:|---:|
| Unfinished fraction (%) | 1 | -6.998 | -6.998 | -6.998 | Unavailable |
| Recorded completed-cohort mean (s) | 1 | -21.012 | -21.012 | -21.012 | Unavailable |
| Recorded completed-cohort p99 (s) | 1 | -12.923 | -12.923 | -12.923 | Unavailable |
| Unique in-evaluation completions / second | 1 | 79.158 | 79.158 | 79.158 | Unavailable |
| Covered-interval mean processing backlog (offsets) | 0 | Unavailable | Unavailable | Unavailable | Unavailable |
| Requested consumer CPU (core-minutes, evaluation + drain) | 1 | 29.779 | 29.779 | 29.779 | Unavailable |

Unfinished-fraction differences are percentage points. With one pair, sample SD is unavailable; with two, it is only a descriptive spread estimate. No confidence interval or significance test is claimed.

## Interpretation limits

Completed-message mean and percentiles are conditional on completion by the fixed drain. Read them with unfinished counts; missing completion latency is never assigned zero. A run p99 or mean of run p99 values is not a pooled-message percentile.
Backlog integrals and means cover only eligible observed intervals. Missing or ownership-transition intervals are not zero, and different coverage can affect the comparison. The displayed offset backlog is not the unique unfinished cohort count.
Cost here is requested consumer-container CPU time, with a common evaluation-through-drain horizon. It is not measured whole-cluster CPU use or a billing total. Startup timing mixes coordinator, Kubernetes and application timestamps; clock and sampling uncertainty remain.
A threshold-hold confirmation means recovery from overload only when backlog was initially above the threshold. In an already healthy low-input trial it is a health check; missing transition observations do not establish failed recovery.
These are short preliminary scheduled-action comparisons on shared nodes, not a tuned HPA evaluation or a completed adaptive-selector study. Ordinary initial assignments and node placements are recorded, not forced equal. Two seed pairs do not justify a strong confidence interval or broad causal/general superiority claim.
Earlier guard-interrupted and pre-production diagnostics remain retained separately. The 99 ms threshold is provisional and the clock probes do not establish its accuracy as an SLA test.

![Paired whole-run outcomes](paired-outcomes.png)

Raw per-message evidence remains under the active code results directory and on Nautilus PVCs. The individual Git run records include evidence hashes, configurations, outcomes, source provenance and monitoring plots. This summary does not replace the raw evidence backup.
