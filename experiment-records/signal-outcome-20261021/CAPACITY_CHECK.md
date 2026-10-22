# Partition input and consumer-capacity check

This extends the signal analysis using the same twenty intervention trials, twelve historical balanced trials and three later deployment-calibration attempts. It adds no experiments. Early-stopped calibration attempts retain their actual boundaries. The windows below are subdivisions of those runs, not independent trials.

## Calculation

Partition input is estimated from changes in `producer_partition_messages_total`, summed over all producers. The counter is updated after the delivery callback, so this is an acknowledgment-rate proxy for arrival, not an exact broker-arrival timestamp. The pre-action window ends at nominal evaluation second 58 and begins at second 28. Each producer uses its actual scrape elapsed time. A missing interior query can be bridged by the cumulative counter when both fresh endpoints belong to the same process; missing endpoints or resets are rejected.

For sequential processing, task-only capacity is the processing-count increment divided by the processing-time-sum increment, equivalently `1 / mean processing seconds`. A **5 ms illustrative mean gives 200 messages/s for that task**. Effective service rate instead divides completed attempts by elapsed wall time while work remains available. It includes polling, result bookkeeping, lag collection, commits and other runtime overhead outside the application timer.

Nonoverlapping 60-second evaluation intervals after second 60, before production stops. Fresh counters, fixed ownership/incarnation, no resets, valid contiguous backlog; 10-second throughput CV <= 0.10; minimum owner processing backlog >= max(100, 2 * observed msg/s). Backlog checks include the preceding 10 seconds. These sampled conditions are consistent with sustained work availability, not proof of continuous busyness.

Reference estimates match node, CPU/memory requests and the recorded task/loop settings. They do not establish identical software, assignment/commit mode, co-location or current contention. In particular, historical calibration used cooperative assignment while targeted actions use the explicit assignment path. All transfer to intervention trials is therefore conditional.

## Calibration evidence

| Node | CPU / GiB requested | Busy windows | Task-only rate in busy windows | Effective rate | Busy-window range | Highest sustained rate in one calibration |
|---|---:|---:|---:|---:|---:|---:|
| edex.calit2.optiputer.net | 2 / 1 | 0 | Unmeasured | Unmeasured | Unmeasured | 499.36 |
| k8s-usra-01.calit2.optiputer.net | 2 / 1 | 0 | Unmeasured | Unmeasured | Unmeasured | 499.81 |
| unseenu.crbs.ucsd.edu | 2 / 1 | 112 | 313.32 | 291.55 | 283.33–302.97 | 296.82 |
| k8s-chase-ci-01.calit2.optiputer.net | 1.25 / 0.625 | 0 | Unmeasured | Unmeasured | Unmeasured | 232.81 |
| k8s-chase-ci-07.calit2.optiputer.net | 1.25 / 0.625 | 6 | 232.69 | 213.81 | 213.52–214.17 | 213.81 |
| edex.calit2.optiputer.net | 1.25 / 0.625 | 0 | Unmeasured | Unmeasured | Unmeasured | 233.39 |
| k8s-chase-ci-01.calit2.optiputer.net | 2 / 0.75 | 0 | Unmeasured | Unmeasured | Unmeasured | 230.62 |
| k8s-chase-ci-07.calit2.optiputer.net | 2 / 0.75 | 1 | 233.44 | 213.93 | 213.93–213.93 | 213.93 |
| edex.calit2.optiputer.net | 2 / 0.75 | 0 | Unmeasured | Unmeasured | Unmeasured | 232.47 |
| unseenu.crbs.ucsd.edu | 2 / 0.75 | 0 | Unmeasured | Unmeasured | Unmeasured | 200.24 |
| k8s-chase-ci-04.calit2.optiputer.net | 2 / 0.75 | 0 | Unmeasured | Unmeasured | Unmeasured | 199.89 |

Rates are messages/s. The effective estimate pools completed counts over observed seconds in eligible windows; task-only capacity pools processing counts over processing seconds. The range is observed variation, not a confidence interval. A sustained rate in an underloaded interval demonstrates work completed; it is **not the consumer’s maximum capacity**. It is the highest per-calibration pooled rate across valid stable windows, not a single peak scrape.

## Reassignment within three consumers

| Workload | Run | Initial input to C2 | Candidate C0 input | Candidate C1 input | Candidate C2 input | Reference screen |
|---|---:|---:|---:|---:|---:|---|
| 12hot-700 | 1 | 582.73 | 237.07 | 231.40 | 231.43 | Conditional support |
| 12hot-700 | 2 | 580.27 | 231.83 | 235.33 | 232.90 | Conditional support |
| 4hot-700 | 1 | 600.10 | 329.77 | 188.63 | 181.53 | Conditional support |
| 4hot-700 | 2 | 603.10 | 329.37 | 185.93 | 184.70 | Conditional support |
| 4hot-600 | 1 | 515.33 | 282.03 | 161.77 | 156.07 | Incomplete calibration |
| 4hot-600 | 2 | 512.50 | 278.30 | 158.17 | 163.53 | Incomplete calibration |

The historical hot owner has a measured mean service rate of about 292 messages/s. Its initial input of about 580–603 messages/s exceeds that reference, whereas redistribution reduces its input to about 182–233 messages/s. The other two historical consumers handled about 499–500 messages/s each in calibration without meeting the backlogged-window criterion; the proposed loads are below that demonstrated rate. This is consistent with the observed recovery after redistribution, without identifying every consumer’s capacity ceiling.

For the revised 600-message/s block, the matching calibration supports about 214 messages/s on the node hosting C1, based on only one eligible minute at the matching allocation. C2 previously sustained about 200 messages/s while underloaded, above its proposed 156–164 messages/s. C0 would receive 278–282 messages/s, but the matching calibration demonstrated only about 232 messages/s and did not saturate it. **That gap means capacity is unmeasured at the required load; it does not show that the assignment is infeasible.** The successful experiment outcomes are retained unchanged.

The CSV includes initial and final-map checks for every trial. Recorded resource histories identify added consumers, but not every node and allocation has a matching backlogged calibration. Scaling therefore cannot be certified by this screen. The targeted candidate maps were specified before their actions and match the observed final maps. Native Kafka final maps are retrospective, not choices known before scaling.

## Interpretation and next measurement

For a candidate assignment A, sum the measured rates of its partitions: lambda_c(A) = sum(lambda_p). A steady-input screen requires lambda_c(A) below effective capacity for every consumer. An added safety margin is a policy choice that still requires calibration. Under heterogeneous application work, use partition-specific service demand rather than assuming equal message costs.

Positive spare service is also needed to drain accumulated work. Passing an average-rate screen does not establish a recovery time, tail-latency target, deadline guarantee or acceptable intervention cost. These must be evaluated separately, including variability and handover time.

Next, measure the uncalibrated nodes under sustained available work with the same assignment/commit path, source and resources intended for intervention. Retain separate validation workloads. This is more informative than treating every sampled window as an independent training example or fitting a deep model to the present few conditions.

[Full evidence and method](capacity-analysis.json) · [Per-consumer assignment checks](assignment-capacity.csv)
