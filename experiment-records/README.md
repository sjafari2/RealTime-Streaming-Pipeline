# Experimental results

This index contains the **32 performance trials reported in the proposal**. One trial is one complete experiment; each condition was run twice. Preparations and technical checks are not counted as performance trials.

| Experiment | Trials | Total time per trial | Results |
|---|---:|---:|---|
| Balanced input at six aggregate rates | 12 | 23 min | [Tables and lag plot](stability-fixed3-20260921/README.md) |
| 80/20: 80% of input across 12 of 60 partitions | 8 | 13 min | [Four-response comparison](four-condition-20260929/README.md) |
| 80% of input across 4 of 60 partitions, 700 msg/s | 4 | 13 min | [Two-response comparison](four-hot-partitions-20260930/README.md) |
| Four high-input partitions, revised deployment at 600 msg/s | 8 | 13 min | [Four-response comparison](four-hot600-20261021/README.md) |

The balanced trials established 700 messages/s as the reference rate for the historical skew comparisons. The additional block uses 600 messages/s after changes to resources and placement, with separate balanced recalibration. Those comparisons evaluate scheduled actions, including their transition and resource costs. The results do not demonstrate a complete adaptive controller or hot-key splitting.

The [combined four-partition tables](four-hot-rate-comparison-20261021/README.md) place the 700 and 600 messages/s configurations side by side and report per-run percentage changes in throughput, mean total backlog, p99, SLO violations and requested resources. These reuse the trials above; they do not add trials or pool the two configurations.

[Detailed intervention metrics and plots](result-metric-audit-20260930/README.md) include throughput, CPU, memory, lag, growth, skew, completion latency and unfinished work. Missing observations remain gaps. The first twelve-partition baseline lacks full-window resource-request coverage.

The retrospective [node-placement review](node-placement-review.json) records observed co-location in the six scaling trials. Original producers and consumers sometimes shared nodes; the stricter [placement requirement](../docs/node-placement.md) applies to subsequent trials and does not change these results.

Run 1 and Run 2 columns refer to separate trials of each response. P99 is a whole-cohort percentile among completed evaluation messages, not an average of dashboard values. Whole-run latency tables report unfinished work alongside it. See [methodology](../docs/experiment-methodology.md), [metric definitions](../docs/metric-definitions.md) and [data availability](../docs/data-and-reproducibility.md).

Earlier campaigns, supplementary trials and superseded reports are preserved on [archive/pre-cleanup-20261008](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008/experiment-records). They use different designs or deployment stages and are not pooled into these reported trials. This selection follows the main proposal, not the direction of the outcomes.

The separate [October 20 resource and deployment calibration](resource-sizing-20261020/README.md) records the replacement producers, reduced allocations and a short balanced test. It is not pooled with the historical 24-trial set. The [October 21 capacity recalibration](resource-calibration-20261021/README.md) records subsequent early-stopped checks at 700 messages/s and a lower-rate check; these also remain separate from the historical performance comparisons.
