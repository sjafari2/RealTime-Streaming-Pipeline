# Four-condition comparison — in progress

Eight performance trials are planned at 700 aggregate messages/s: keep three consumers, redistribute within three, scale to six with targeted redistribution, and scale to six with Kafka's normal rebalance. Each condition runs twice with 1 minute warm-up, 10 minutes evaluation, and 2 minutes drain.

The monitoring correction, two empty-topic ownership checks, and a native-scaling technical validation passed. The starting map and twelve hot partitions are frozen for all trials; initially all twelve hot partitions belong to Consumer 2.

**Completed: baseline, Run 1.** Of 420,000 acknowledged evaluation messages, 274,509 completed by the cutoff and 145,491 remained unfinished (34.64%). Recorded completion p99 was 366.64 seconds. **Redistribution, Run 1**, also completed: all 420,000 evaluation messages finished, with p99 80.10 seconds. The observed application-processing pause was 14.54 seconds. Backlog stayed at or below 100 offsets for thirty seconds while input continued, confirmed 305.18 seconds after the decision. The stricter 50-offset recovery threshold was not met.

The Mac coordinating this baseline entered idle sleep for fifteen minutes. Nautilus continued its frozen workload schedule. Historical Prometheus data were recovered without rerunning traffic; the original manifest and cohort-summary hashes are unchanged. Lag coverage is 99.67%, and message/offset validation passed. Local resource-request observations during sleep remain unavailable and are not interpolated. A stronger temporary system-sleep assertion is enabled on AC power for the remaining trials.

The recovered baseline is retained exactly once; two performance trials are verified and six remain. The results above are preliminary within this ongoing block. Technical checks are excluded from the performance count. The full protocol, analysis code, and raw evidence preserve these distinctions.

[Protocol](../../experiments/four-condition-20260929/README.md) · [Collection-recovery record](baseline-collection-recovery.json) · [Monitoring correction](../monitoring-gap-20260929/README.md)
