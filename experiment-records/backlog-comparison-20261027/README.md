# Starting backlog and mitigation outcomes

Status: provisional. Completed trials: 6 of 16.

Every evaluation uses 600 total messages/s, with 80% sent to twelve of sixty partitions. Different warm-up rates establish two backlog conditions. These histories also change the age of queued records; they are not a pure manipulation of queue size.

| Starting backlog | Response | Run | Completion p99 (s) | Unfinished (%) | Recovery confirmed (s) | Requested CPU (core-min) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Larger | Keep 3 | 1 | 394.372 | 38.49 | Unconfirmed | 72.0 |
| Larger | Redistribute within 3 | 1 | 110.500 | 0.00 | 420.0 | 72.0 |
| Smaller | Keep 3 | 1 | 337.315 | 30.71 | Unconfirmed | 72.0 |
| Smaller | Redistribute within 3 | 1 | 53.109 | 0.00 | 236.0 | 72.0 |
| Smaller | Scale + targeted redistribution | 1 | 78.340 | 0.00 | 158.0 | 136.6 |
| Smaller | Scale + Kafka rebalance | 1 | 54.444 | 0.00 | Unconfirmed | 136.8 |

Whole evaluation-production cohort, earliest completion by the drain cutoff; unfinished records are separate.
Before: seconds 30–60; after: verified activation to 600; final: 570–600. Throughput counts distinct completions in the interval; window latency uses evaluation messages completed there. All exclude drain.
Equal-weight valid raw observations within each stated interval. Rolling signal plots retain their separately defined 15-observation and 15-interval windows.
Processing backlog at most 100 offsets for 30 continuous valid seconds during continuing input; elapsed time starts at the common scheduled decision.
Individual results retained; no pooled p99 or average of rolling percentiles.

The pending-clearance curve uses all records acknowledged and still unfinished at the common decision time, including warm-up records. A plateau below 100% retains unfinished work in the denominator. The dashed line at 540 seconds marks production ending; the remaining 120 seconds are drain.

Capacity estimates apply to the original three pods under the saved busy calibration. Added consumers remain uncalibrated. Two repetitions per condition are planned; this report contains only the completed trials listed above. Limited repetitions and shared-node variability constrain interpretation.
