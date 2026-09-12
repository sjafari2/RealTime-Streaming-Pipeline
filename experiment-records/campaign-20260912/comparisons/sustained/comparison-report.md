# Preliminary action comparison

Status: **partial_one_of_two_predeclared_pairs**. Each pair below uses its own matched workload seed. Short, sustained and isolated-partition episodes are analyzed separately.

## Pair L1 — seed 21

Keep three consumers: `run-20260912-070155`. Schedule three to six: `run-20260912-064358`.

| Measure | Keep 3 | Schedule 3 → 6 |
|---|---:|---:|
| Distinct evaluation admissions | 810,000 | 810,000 |
| Unfinished at drain | 92,148 | 0 |
| Unfinished fraction (%) | 11.376 | 0.000 |
| Recorded completed-cohort mean (s) | 56.912 | 15.467 |
| Recorded completed-cohort p99 (s) | 292.424 | 95.580 |
| Admissions / evaluation second | 1,500.000 | 1,500.000 |
| Unique in-evaluation completions / second | 1,239.626 | 1,490.231 |
| Covered-interval mean processing backlog (offsets) | 85,073.3 | 20,994.2 |
| Lag coverage (%) | 99.63 | 93.70 |
| Requested consumer CPU (core-minutes, evaluation + drain) | 66.000 | 125.749 |
| Resource coverage for that horizon (%) | 100.00 | 100.00 |

Compatibility checks: passed (workload configuration, action settings and recorded application/Python/package signatures). Both full message-outcome evidence checks passed: True.
Keep 3 quality flags: none at the predeclared coverage screens. Recovery: `{"censored": true, "followup_seconds": 480.0, "hold_seconds": 20, "seconds_to_confirmation": null, "status": "not_observed_by_evaluation_end", "threshold_offsets": 1500}`.
Scale quality flags: none at the predeclared coverage screens. Recovery: `{"censored": true, "followup_seconds": 480.0, "hold_seconds": 20, "seconds_to_confirmation": null, "status": "not_observed_by_evaluation_end", "threshold_offsets": 1500}`.

| New consumer | Node | Request → Kubernetes Ready (s) | Request → first recorded completion (s) |
|---|---|---:|---:|
| consumer-sts-3 | k8s-chase-ci-07.calit2.optiputer.net | 17.86 | 20.46 |
| consumer-sts-4 | k8s-gpu-01.calit2.optiputer.net | 14.86 | 17.31 |
| consumer-sts-5 | k8s-ravi-01.calit2.optiputer.net | 6.86 | 9.20 |

Source provenance: `run-20260912-070155` launched from Git revision `4abddcfe87cc19168523b85ed5ebfd13565e01c2` with recorded working-tree changes. Its launch record retains the exact status; the matched application signatures are checked separately.
The uncommitted changes were the single/batch HPA pause-and-restore wrapper, its documentation and tests, later committed as `6644c86`. They were present before this launcher started. This campaign already owned its HPA pause and used the direct scheduled-run path; the producer/consumer application code matched the paired run. The later commit is not represented as the revision that existed at launch.

![Pair L1 monitoring curves](pair-L1-timecourse.png)

## Interpretation limits

Completed-message mean and percentiles are conditional on completion by the fixed drain. Read them with unfinished counts; missing completion latency is never assigned zero. A run p99 or mean of run p99 values is not a pooled-message percentile.
Backlog integrals and means cover only eligible observed intervals. Missing or ownership-transition intervals are not zero, and different coverage can affect the comparison. The displayed offset backlog is not the unique unfinished cohort count.
Cost here is requested consumer-container CPU time, with a common evaluation-through-drain horizon. It is not measured whole-cluster CPU use or a billing total. Startup timing mixes coordinator, Kubernetes and application timestamps; clock and sampling uncertainty remain.
These are short preliminary scheduled-action comparisons on shared nodes, not a tuned HPA evaluation or a completed adaptive-selector study. Ordinary initial assignments and node placements are recorded, not forced equal. Two seed pairs do not justify a strong confidence interval or broad causal/general superiority claim.
Earlier guard-interrupted and pre-production diagnostics remain retained separately. The 99 ms threshold is provisional and the clock probes do not establish its accuracy as an SLA test.

![Paired whole-run outcomes](paired-outcomes.png)

Raw per-message evidence remains under the active code results directory and on Nautilus PVCs. The individual Git run records include evidence hashes, configurations, outcomes, source provenance and monitoring plots. This summary does not replace the raw evidence backup.
