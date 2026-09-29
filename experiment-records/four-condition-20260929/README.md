# Four-condition comparison — in progress

Eight performance trials are planned at 700 aggregate messages/s: keep three consumers, redistribute within three, scale to six with targeted redistribution, and scale to six with Kafka's normal rebalance. Each condition runs twice with 1 minute warm-up, 10 minutes evaluation, and 2 minutes drain.

The monitoring correction, two empty-topic ownership checks, and a native-scaling technical validation passed. The starting map and twelve hot partitions are frozen for all trials; initially all twelve hot partitions belong to Consumer 2.

**Run 1 is verified for all four conditions.** Each admitted 420,000 evaluation messages.

| Condition | Completion p99 (s) | Unfinished (%) | Recovery confirmed after decision (s) |
|---|---:|---:|---:|
| Keep three | 366.64 | 34.64 | No intervention |
| Redistribute within three | 80.10 | 0 | 305.18 |
| Scale plus targeted redistribution | 100.81 | 0 | 168.98 |
| Scale with Kafka rebalance | 71.41 | 0 | 259.01 |

Recovery uses the predeclared threshold: processing backlog at most 100 offsets for thirty consecutive valid seconds while input continues. Explicit redistribution paused application processing for 14.54 seconds; the combined intervention paused it for 19.50 seconds. Under native scaling, the longest observed pipeline-wide interval without processing during the measured transition was 0.092 seconds; this does not establish uninterrupted processing for each partition. Native scaling left hot-partition counts of 0, 0, 2, 4, 2, 4 across Consumers 0-5. The targeted six-consumer condition assigned two hot partitions to each consumer.

The fixed-three redistribution trial used 72.00 requested core-minutes over evaluation plus drain, compared with 136.86 for the combined intervention and 137.15 for native scaling. Baseline Run 1 has incomplete request observations, described below. One trial per condition is insufficient to establish a reliable ranking.

The Mac coordinating this baseline entered idle sleep for fifteen minutes. Nautilus continued its frozen workload schedule. Historical Prometheus data were recovered without rerunning traffic; the original manifest and cohort-summary hashes are unchanged. Lag coverage is 99.67%, and message/offset validation passed. Local resource-request observations during sleep remain unavailable and are not interpolated. A stronger temporary system-sleep assertion is enabled on AC power for the remaining trials.

The recovered baseline is retained exactly once; four performance trials are verified and four remain. The results above are preliminary within this ongoing block. Technical checks are excluded from the performance count. The full protocol, analysis code, and raw evidence preserve these distinctions.

[Protocol](../../experiments/four-condition-20260929/README.md) · [Collection-recovery record](baseline-collection-recovery.json) · [Monitoring correction](../monitoring-gap-20260929/README.md)
