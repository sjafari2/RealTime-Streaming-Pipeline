# Current project status

Updated 29 September 2026. Active code and documentation are on `main`.

## Completed evidence

The inventory contains **56 performance trials**. Technical checks and failed preparations are recorded separately.

| Experimental block | Trials | Evidence |
|---|---:|---|
| Original keep-three versus scale-to-six comparisons | 24 | [Per-run table](../experiment-records/paired-table-20260914/paired-results-data.json) |
| Balanced fixed-three stability, 600-1,500 aggregate messages/s | 12 | [Results and rate plots](../experiment-records/stability-fixed3-20260921/COMPLETED_RESULTS.md) |
| Distributed and concentrated hot-partition ownership at 700 messages/s | 4 | [Ownership comparison](../experiment-records/hot-ownership-consumers-20260927/README.md) |
| Keep three versus scaling with targeted redistribution, eight-minute trials | 4 | [Combined-intervention results](../experiment-records/c2-scaling-20260927/README.md) |
| Keep three versus redistribution within three, eight-minute trials | 4 | [Redistribution results](../experiment-records/c2-redistribution-20260928/README.md) |
| Four scheduled responses, thirteen-minute trials | 8 | [Latest comparison and all plots](../experiment-records/four-condition-20260929/README.md) |

Balanced calibration showed low bounded lag over the observed intervals at 600, 700 and 800 messages/s. At 900, backlog grew during production but the evaluation messages completed during drain; 1,200 and 1,500 produced growing backlog and unfinished work. The later comparisons therefore use 700 aggregate messages/s, a rate calibrated under balanced input. Concentrating twelve hot partitions on Consumer 2 can still overload that consumer at this aggregate rate. These measurements do not establish permanent stability or a universal capacity boundary.

The latest block compares keeping three consumers, redistributing within three, scaling with targeted redistribution, and scaling with Kafka's normal rebalance. Each condition runs twice with one minute warm-up, ten minutes evaluation and two minutes drain. All start with the same verified ownership and original consumer placement. The [report](../experiment-records/four-condition-20260929/README.md) gives per-run completion outcomes, recovery, interruption, resource use and observed assignments; it does not infer a winning adaptive policy.

## Monitoring and validation

The long post-handoff offset-initialization gap was diagnosed and corrected before this block. Before a partition first returns records, the monitor can use its verified assignment-start offset when the Kafka client position is unresolved. The fallback is removed on record return or a resolved client position. Ownership, freshness, retained-range and watermark checks remain in place. Genuine handoff gaps are retained. See the [diagnosis and live verification](../experiment-records/monitoring-gap-20260929/README.md).

The latest code passed 286 tracked local tests. Empty-topic preparations, a nonempty monitoring check and a native-scaling technical trial preceded the performance comparisons. Completed trials retain acknowledgment/completion reconciliation, offset and identity validation, runtime source hashes, package versions and monitoring exports. Those checks concern the synthetic application endpoint, before commit acknowledgment; they do not establish exactly-once external effects.

The Mac coordinating the first baseline slept during collection. Nautilus continued the saved schedule; historical Prometheus evidence was recovered without rerunning traffic or changing cohort results. Missing local resource-request samples remain unavailable. The report separates this partial integral from complete resource observations. Original configuration restoration and stopped applications are verified after the block.

## Work still to evaluate

The implemented actions are scheduled treatments. An adaptive decision policy, producer-side hot-key splitting, moving hotspots, changing processing costs, larger deployments and application-specific correctness remain future work. Further comparisons should test whether capacity-aware redistribution and intervention timing improve on the relevant existing responses under comparable information and resource budgets. Repetitions remain limited, and matching initial pods and ownership does not eliminate variability on shared machines.

The [experiment register](../EXPERIMENT_REGISTER.md) is the concise progress record. Earlier storage-recovery and preparation documents retain their original dates and are historical records, not current deployment-status claims.
