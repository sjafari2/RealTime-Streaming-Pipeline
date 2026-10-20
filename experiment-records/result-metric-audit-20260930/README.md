# Intervention metrics and figures

[metrics.json](metrics.json) contains detailed outcomes, coverage, costs and source hashes for the twelve current intervention trials. [runtime-states.csv](runtime-states.csv) retains timestamped diagnostic state, including persistent-hot partition IDs. These are generated analysis outputs; they are not edited by hand.

| Block | Performance and resources | Outcomes | Runtime signals |
|---|---|---|---|
| Twelve hot partitions | [PNG](figures/twelve-partition-compact-performance.png) / [PDF](figures/twelve-partition-compact-performance.pdf) | [PNG](figures/twelve-partition-compact-outcomes.png) / [PDF](figures/twelve-partition-compact-outcomes.pdf) | [PNG](figures/twelve-partition-runtime-state.png) / [PDF](figures/twelve-partition-runtime-state.pdf) |
| Four hot partitions | [PNG](figures/four-partition-compact-performance.png) / [PDF](figures/four-partition-compact-performance.pdf) | [PNG](figures/four-partition-compact-outcomes.png) / [PDF](figures/four-partition-compact-outcomes.pdf) | [PNG](figures/four-partition-runtime-state.png) / [PDF](figures/four-partition-runtime-state.pdf) |

Full-evaluation mean total backlog and mean skew are arithmetic averages of valid individual evaluation samples (`sample_mean`). The 15-sample before/end means and the rolling diagnostic signals use their specified windows. Missing samples are excluded, and lag coverage continues to describe valid continuous time.

The twelve-partition summary also includes `pre_mitigation_skew`: the arithmetic average of 15 individual maximum-to-mean partition-lag ratios at evaluation seconds 30, 32, ..., 58, before the scheduled action at second 60. This uses the same window as pre-action backlog, including for Keep 3. All 15 samples must be valid and contiguous, with unchanged ownership and nondecreasing offsets. It is neither a ratio of averaged lags nor an average of overlapping rolling means. The verified values and source hashes are retained in [pre-action-skew.json](../four-condition-20260929/pre-action-skew.json).

Plots retain unavailable intervals as gaps. Runtime signals are reconstructed diagnostics, not observations of an adaptive controller. Late-cohort analysis was retrospective for the twelve-partition block and specified before the four-partition trials. Full-window completion outcomes remain the primary comparison.

[Analysis commands](../../experiments/result-metric-audit-20260930/README.md) · [Metric definitions](../../docs/metric-definitions.md) · [All results](../README.md)
