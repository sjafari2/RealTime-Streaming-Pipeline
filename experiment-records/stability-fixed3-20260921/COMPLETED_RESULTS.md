# Balanced stability trials: completed results

Twelve validated trials provide two runs at each aggregate input rate: 600, 700, 800, 900, 1,200 and 1,500 messages/s. All used three consumers, three producers, 60 partitions, balanced input, 2,000 SHA-256 iterations per message, no added processing sleep and no scaling. Each trial had 60 seconds of warm-up, 1,200 seconds of evaluation production and 120 seconds of drain (23 minutes total, excluding preparation and collection).

[Combined lag figure](all-balanced-rates-lag.png) · [PDF](all-balanced-rates-lag.pdf) · [Figure input identities and hashes](all-balanced-rates-lag.json) · [Full comparison data](comparison-data.json)

| Aggregate input (messages/s) | Run | Mean lag (offsets) | Peak sampled lag | Final 10-minute backlog growth (offsets/s) | Unfinished after drain | Completion p99 (s) | Plot |
|---:|---:|---:|---:|---:|---:|---:|---|
| 600 | 1 | 42.5 | 132 | -0.01 | 0 (0.00%) | 0.237 | [PNG](first-run-lag.png) / [PDF](first-run-lag.pdf) |
| 600 | 2 | 43.1 | 76 | 0.02 | 0 (0.00%) | 0.197 | [PNG](rate600-run2-lag.png) / [PDF](rate600-run2-lag.pdf) |
| 700 | 1 | 53.4 | 117 | 0.00 | 0 (0.00%) | 0.248 | [PNG](rate700-run1-lag.png) / [PDF](rate700-run1-lag.pdf) |
| 700 | 2 | 54.4 | 221 | -0.02 | 0 (0.00%) | 0.395 | [PNG](rate700-run2-lag.png) / [PDF](rate700-run2-lag.pdf) |
| 800 | 1 | 68.5 | 449 | 0.01 | 0 (0.00%) | 0.585 | [PNG](rate800-run1-lag.png) / [PDF](rate800-run1-lag.pdf) |
| 800 | 2 | 60.7 | 177 | -0.02 | 0 (0.00%) | 0.332 | [PNG](rate800-run2-lag.png) / [PDF](rate800-run2-lag.pdf) |
| 900 | 1 | 7,319.5 | 14,166 | 11.48 | 0 (0.00%) | 47.400 | [PNG](rate900-run1-lag.png) / [PDF](rate900-run1-lag.pdf) |
| 900 | 2 | 7,705.2 | 15,315 | 12.69 | 0 (0.00%) | 51.411 | [PNG](rate900-run2-lag.png) / [PDF](rate900-run2-lag.pdf) |
| 1,200 | 1 | 73,617.4 | 139,688 | 109.86 | 104,668 (7.27%) | 346.338 | [PNG](rate1200-run1-lag.png) / [PDF](rate1200-run1-lag.pdf) |
| 1,200 | 2 | 68,555.2 | 130,246 | 102.93 | 93,668 (6.50%) | 333.600 | [PNG](rate1200-run2-lag.png) / [PDF](rate1200-run2-lag.pdf) |
| 1,500 | 1 | 136,227.5 | 259,643 | 205.54 | 224,986 (12.50%) | 553.561 | [PNG](rate1500-run1-lag.png) / [PDF](rate1500-run1-lag.pdf) |
| 1,500 | 2 | 137,665.1 | 261,155 | 205.06 | 227,140 (12.62%) | 549.487 | [PNG](rate1500-run2-lag.png) / [PDF](rate1500-run2-lag.pdf) |

Both runs at 600, 700 and 800 messages/s showed low fluctuating lag without sustained upward accumulation over the observed 20-minute production interval, and all evaluation messages finished by the drain cutoff. Both 900 messages/s runs accumulated lag during production, although all evaluation messages finished during drain. At 1,200 messages/s, both runs accumulated substantial lag and left 6.50–7.27% unfinished; at 1,500 messages/s, both left 12.50–12.62% unfinished. Finishing during drain does not establish stability under continuing input.

The final three repeats completed with producer/consumer outcome checks and process CPU/RSS measurement audits passing, and pre-campaign configuration restoration verified. Their p99 values were 0.395 s at 700, 0.332 s at 800 and 333.600 s at 1,200 messages/s; unfinished counts were respectively 0, 0 and 93,668. Mean lag and peak lag are not latency measures.

All twelve recorded initial partition-to-consumer assignments matched when compared after execution. These stability trials recorded assignments and checked readiness; they did not enforce a frozen ownership reference before releasing traffic. Matching partition owners does not establish identical machine performance or eliminate shared-infrastructure variation. No new machine-placement constraint was added.

Figures show evaluation only. The 600–800 panels share a 0–500 vertical scale; the higher-rate panels use different scales. Lag is high offset minus returned-record position, rather than committed-offset lag. Mean lag is time-weighted over valid adjacent observations. Growth uses processing backlog and same-owner covered intervals in the final ten evaluation minutes. Evaluation lag coverage was 99.33% for 700 Run 2 and 800 Run 1, and 99.83% for the other runs. Missing data remain gaps, not zero backlog. The latest three trials each retained 99.83% evaluation coverage for every producer/consumer process CPU and RSS series. These measurements do not represent whole-node, broker or container utilization.

Completion p99 concerns admitted evaluation messages completed before the drain cutoff and is reported alongside unfinished work. Completion is measured before offset-commit acknowledgment. Warm-up messages are excluded from the evaluation cohort; their backlog, if any, is retained as part of the initial system state.

These are finite observations with two repetitions per rate. They do not establish a universal capacity threshold, infinite-horizon stability, or the cause of any per-consumer bottleneck. The replacement Kafka deployment differs from the earlier 24-trial campaign. Original collection/analysis failures remain preserved for recovered trials; the [800 recovery report](RATE800_RECOVERY.md) and [900 repeat report](RATE900_REPEAT.md) retain their detailed histories. No additional trial was launched during post-processing.

Raw evidence is retained separately; these versioned summaries and hashes do not replace a raw-data backup. Per-run records for the latest repeats are [700](rate700-run2.json), [800](rate800-run2.json) and [1,200](rate1200-run2.json).
