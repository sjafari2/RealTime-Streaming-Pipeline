# Intervention metrics and figures

[metrics.json](metrics.json) contains detailed outcomes, coverage, costs and source hashes for the twelve current intervention trials. [runtime-states.csv](runtime-states.csv) retains timestamped diagnostic state, including persistent-hot partition IDs. These are generated analysis outputs; they are not edited by hand.

| Block | Performance and resources | Outcomes | Runtime signals |
|---|---|---|---|
| Twelve hot partitions | [PNG](figures/twelve-partition-compact-performance.png) / [PDF](figures/twelve-partition-compact-performance.pdf) | [PNG](figures/twelve-partition-compact-outcomes.png) / [PDF](figures/twelve-partition-compact-outcomes.pdf) | [PNG](figures/twelve-partition-runtime-state.png) / [PDF](figures/twelve-partition-runtime-state.pdf) |
| Four hot partitions | [PNG](figures/four-partition-compact-performance.png) / [PDF](figures/four-partition-compact-performance.pdf) | [PNG](figures/four-partition-compact-outcomes.png) / [PDF](figures/four-partition-compact-outcomes.pdf) | [PNG](figures/four-partition-runtime-state.png) / [PDF](figures/four-partition-runtime-state.pdf) |

Plots retain unavailable intervals as gaps. Runtime signals are reconstructed diagnostics, not observations of an adaptive controller. Late-cohort analysis was retrospective for the twelve-partition block and specified before the four-partition trials. Full-window completion outcomes remain the primary comparison.

[Analysis commands](../../experiments/result-metric-audit-20260930/README.md) · [Metric definitions](../../docs/metric-definitions.md) · [All results](../README.md)
