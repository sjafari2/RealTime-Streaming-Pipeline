# Experiment protocols

The active protocols reproduce the designs represented in the [32-trial result set](../experiment-records/README.md). Preview commands print a design without starting cluster traffic. Execution requires the configured deployment, synchronized source, a committed checkout and successful readiness checks.

| Protocol | Workload and responses | Trials |
|---|---|---:|
| [Balanced rates](stability-fixed3-20260921/README.md) | Six aggregate rates, three fixed consumers | 12 |
| [Twelve hot partitions](four-condition-20260929/README.md) | Keep three, redistribute, targeted scaling, native scaling | 8 |
| [Four hot partitions](four-hot-partitions-20260930/README.md) | Redistribute within three versus targeted scaling to six | 4 |
| [Four hot partitions at 600 messages/s](four-hot600-20261021/README.md) | All four responses after deployment recalibration | 8 |

[Metric analysis](result-metric-audit-20260930/README.md) produces the intervention summaries and figures from retained raw evidence. The [monitoring check](monitoring-gap-20260929/README.md) validates a required measurement path before a new intervention campaign. Technical checks are not performance trials.

These are bounded research protocols. A new workload, cluster or application requires calibration and a recorded design; changing a configuration does not retrospectively change the published evidence.

The [signal-to-outcome analysis](../experiment-records/signal-outcome-20261021/README.md) joins observations before action to the outcomes of the twenty intervention trials. It is an offline analysis of existing evidence and adds no trials.
