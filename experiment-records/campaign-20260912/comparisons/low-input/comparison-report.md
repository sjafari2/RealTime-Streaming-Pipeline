# Preliminary action comparison

Status: **complete_predeclared_family**. Each pair below uses its own matched workload seed. Short, sustained, concentrated-partition and low-input families are analyzed separately.

## Pair A1 — seed 51

Keep three consumers: `run-20260912-094344`. Schedule three to six: `run-20260912-095159`.

| Measure | Keep 3 | Schedule 3 → 6 |
|---|---:|---:|
| Distinct evaluation admissions | 72,000 | 72,000 |
| Unfinished at drain | 0 | 0 |
| Unfinished fraction (%) | 0.000 | 0.000 |
| Recorded completed-cohort mean (s) | 0.169 | 0.173 |
| Recorded completed-cohort p99 (s) | 0.824 | 0.939 |
| Admissions / evaluation second | 600.000 | 600.000 |
| Unique in-evaluation completions / second | 599.642 | 600.200 |
| Covered-interval mean processing backlog (offsets) | 81.4 | 72.8 |
| Lag coverage (%) | 98.33 | 91.67 |
| Requested consumer CPU (core-minutes, evaluation + drain) | 24.000 | 41.776 |
| Resource coverage for that horizon (%) | 100.00 | 100.00 |

Compatibility checks: passed (workload configuration, action settings and recorded application/Python/package signatures). Both full message-outcome evidence checks passed: True.
Keep 3 quality flags: none at the predeclared coverage screens. Backlog-threshold diagnostic: `{"censored": false, "confirmed_epoch": 1789228023.804, "hold_seconds": 20, "qualifying_interval_start_epoch": 1789228003.804, "seconds_to_confirmation": 21.999847888946533, "status": "recovered", "threshold_offsets": 1500}`.
Scale quality flags: none at the predeclared coverage screens. Backlog-threshold diagnostic: `{"censored": false, "confirmed_epoch": 1789228536.929, "hold_seconds": 20, "qualifying_interval_start_epoch": 1789228516.929, "seconds_to_confirmation": 40.000027894973755, "status": "recovered", "threshold_offsets": 1500}`.

| Condition | Valid pre-action backlog range (offsets) | Valid full-evaluation backlog range (offsets) |
|---|---:|---:|
| Keep 3 | 54–200 | 50–200 |
| Scale | 50–151 | 10–153 |

The saved analyzer status `recovered` denotes the defined backlog-threshold hold. When pre-action backlog was already below the threshold, interpret its confirmation as a health check, not recovery from observed prior overload. Ranges above describe valid samples only; they do not fill missing transition intervals or establish a universal capacity margin.


| New consumer | Node | Request → Kubernetes Ready (s) | Request → first recorded completion (s) |
|---|---|---:|---:|
| consumer-sts-3 | k8s-u200-00.calit2.optiputer.net | 8.91 | 11.92 |
| consumer-sts-4 | k8s-ravi-01.calit2.optiputer.net | 11.91 | 14.11 |
| consumer-sts-5 | k8s-chase-ci-10.calit2.optiputer.net | 8.91 | 12.04 |

![Pair A1 monitoring curves](pair-A1-timecourse.png)

## Pair A2 — seed 52

Keep three consumers: `run-20260912-100910`. Schedule three to six: `run-20260912-100035`.

| Measure | Keep 3 | Schedule 3 → 6 |
|---|---:|---:|
| Distinct evaluation admissions | 72,000 | 72,000 |
| Unfinished at drain | 0 | 0 |
| Unfinished fraction (%) | 0.000 | 0.000 |
| Recorded completed-cohort mean (s) | 0.178 | 0.163 |
| Recorded completed-cohort p99 (s) | 0.825 | 0.900 |
| Admissions / evaluation second | 600.000 | 600.000 |
| Unique in-evaluation completions / second | 599.733 | 600.000 |
| Covered-interval mean processing backlog (offsets) | 86.9 | 63.3 |
| Lag coverage (%) | 98.33 | 93.33 |
| Requested consumer CPU (core-minutes, evaluation + drain) | 24.000 | 41.729 |
| Resource coverage for that horizon (%) | 100.00 | 100.00 |

Compatibility checks: passed (workload configuration, action settings and recorded application/Python/package signatures). Both full message-outcome evidence checks passed: True.
Keep 3 quality flags: none at the predeclared coverage screens. Backlog-threshold diagnostic: `{"censored": false, "confirmed_epoch": 1789229610.882, "hold_seconds": 20, "qualifying_interval_start_epoch": 1789229590.882, "seconds_to_confirmation": 21.99976396560669, "status": "recovered", "threshold_offsets": 1500}`.
Scale quality flags: none at the predeclared coverage screens. Backlog-threshold diagnostic: `{"censored": false, "confirmed_epoch": 1789229047.881, "hold_seconds": 20, "qualifying_interval_start_epoch": 1789229027.881, "seconds_to_confirmation": 37.99970102310181, "status": "recovered", "threshold_offsets": 1500}`.

| Condition | Valid pre-action backlog range (offsets) | Valid full-evaluation backlog range (offsets) |
|---|---:|---:|
| Keep 3 | 14–170 | 14–170 |
| Scale | 36–147 | 22–147 |

The saved analyzer status `recovered` denotes the defined backlog-threshold hold. When pre-action backlog was already below the threshold, interpret its confirmation as a health check, not recovery from observed prior overload. Ranges above describe valid samples only; they do not fill missing transition intervals or establish a universal capacity margin.


| New consumer | Node | Request → Kubernetes Ready (s) | Request → first recorded completion (s) |
|---|---|---:|---:|
| consumer-sts-3 | k8s-u200-00.calit2.optiputer.net | 7.61 | 10.73 |
| consumer-sts-4 | k8s-ravi-01.calit2.optiputer.net | 9.61 | 10.78 |
| consumer-sts-5 | k8s-chase-ci-10.calit2.optiputer.net | 7.61 | 10.76 |

![Pair A2 monitoring curves](pair-A2-timecourse.png)

## Descriptive differences across matched seeds

Each difference is scale minus keep-3 for one matched seed. Means and sample standard deviations below summarize these paired run-level differences; they do not pool message latencies. Coverage screens apply separately to each metric.

| Measure | Eligible pairs | Mean paired difference | Minimum | Maximum | Sample SD |
|---|---:|---:|---:|---:|---:|
| Unfinished fraction (%) | 2 | 0.000 | 0.000 | 0.000 | 0.000 |
| Recorded completed-cohort mean (s) | 2 | -0.006 | -0.016 | 0.004 | 0.014 |
| Recorded completed-cohort p99 (s) | 2 | 0.095 | 0.075 | 0.115 | 0.028 |
| Unique in-evaluation completions / second | 2 | 0.413 | 0.267 | 0.558 | 0.206 |
| Covered-interval mean processing backlog (offsets) | 2 | -16.1 | -23.6 | -8.6 | 10.6 |
| Requested consumer CPU (core-minutes, evaluation + drain) | 2 | 17.753 | 17.729 | 17.776 | 0.033 |

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
