# Experimental results

This index contains the **24 performance trials in the main proposal**. One trial is one complete experiment; each condition was run twice. Preparations and technical checks are not counted as performance trials.

| Experiment | Trials | Total time per trial | Results |
|---|---:|---:|---|
| Balanced input at six aggregate rates | 12 | 23 min | [Tables and lag plot](stability-fixed3-20260921/README.md) |
| 80/20: 80% of input across 12 of 60 partitions | 8 | 13 min | [Four-response comparison](four-condition-20260929/README.md) |
| 80% of input across 4 of 60 partitions | 4 | 13 min | [Two-response comparison](four-hot-partitions-20260930/README.md) |

The balanced trials established 700 messages/s as the reference rate for the skew comparisons. Those comparisons evaluate scheduled actions, including their transition and resource costs. The results do not demonstrate a complete adaptive controller or hot-key splitting.

[Detailed intervention metrics and plots](result-metric-audit-20260930/README.md) include throughput, CPU, memory, lag, growth, skew, completion latency and unfinished work. Missing observations remain gaps. The first twelve-partition baseline lacks full-window resource-request coverage.

Run 1 and Run 2 columns refer to separate trials of each response. P99 is a whole-cohort percentile among completed evaluation messages, not an average of dashboard values. Every table reports unfinished work alongside it. See [methodology](../docs/experiment-methodology.md), [metric definitions](../docs/metric-definitions.md) and [data availability](../docs/data-and-reproducibility.md).

Earlier campaigns, supplementary trials and superseded reports are preserved on [archive/pre-cleanup-20261008](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008/experiment-records). They use different designs or deployment stages and are not pooled into these 24 trials. This selection follows the main proposal, not the direction of the outcomes.
