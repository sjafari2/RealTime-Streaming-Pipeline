# Balanced stability trials: completed results

Later follow-up: the [800 messages/s recovery report](RATE800_RECOVERY.md) adds a validated original trial, its lag plot, CPU/RSS audit and evidence hashes; no replacement trial was required.

Six validated trials used three consumers, three producers, 60 partitions, balanced input, 2,000 SHA-256 iterations per message and no scaling. Each trial had 60 seconds of warm-up, 1,200 seconds of evaluation production and 120 seconds of drain. The recovered 600 messages/s trials and the later campaigns are included together; their original failure records remain available.

| Aggregate input (messages/s) | Run | Mean lag (offsets) | Peak sampled lag | Final 10-minute backlog growth (offsets/s) | Unfinished after drain | Completion p99 (s) | Plot |
|---:|---:|---:|---:|---:|---:|---:|---|
| 600 | 1 | 42.5 | 132 | -0.01 | 0.00% | 0.237 | [PNG](first-run-lag.png) / [PDF](first-run-lag.pdf) |
| 600 | 2 | 43.1 | 76 | 0.02 | 0.00% | 0.197 | [PNG](rate600-run2-lag.png) / [PDF](rate600-run2-lag.pdf) |
| 900 | 1 | 7,319.5 | 14,166 | 11.48 | 0.00% | 47.400 | [PNG](rate900-run1-lag.png) / [PDF](rate900-run1-lag.pdf) |
| 1200 | 1 | 73,617.4 | 139,688 | 109.86 | 7.27% | 346.338 | [PNG](rate1200-run1-lag.png) / [PDF](rate1200-run1-lag.pdf) |
| 1500 | 1 | 136,227.5 | 259,643 | 205.54 | 12.50% | 553.561 | [PNG](rate1500-run1-lag.png) / [PDF](rate1500-run1-lag.pdf) |
| 1500 | 2 | 137,665.1 | 261,155 | 205.06 | 12.62% | 549.487 | [PNG](rate1500-run2-lag.png) / [PDF](rate1500-run2-lag.pdf) |

Both 600 messages/s trials maintained small lag without sustained growth. At 900 messages/s, lag grew slowly during production, although all evaluation messages completed by the end of drain. At 1,200 messages/s, lag grew faster and 7.27% remained unfinished. Both 1,500 messages/s trials showed similar sustained growth and about 12.5–12.6% unfinished. Finishing during drain does not establish stability during continuous production.

Figures show evaluation only and use different vertical scales to keep each trend visible. Lag means high offset minus returned-record position, rather than committed-offset lag. Mean lag is time-weighted over valid adjacent observations. Growth uses processing backlog and same-owner covered intervals in the final ten evaluation minutes. All six evaluation lag coverage fractions were 99.83%; missing data are not treated as zero. Completion p99 concerns completed admitted evaluation messages only and is shown alongside unfinished work.

These are finite observations on shared infrastructure, with one trial each at 900 and 1,200 messages/s and two each at 600 and 1,500 messages/s. They do not establish a universal capacity threshold, infinite-horizon stability, or the cause of any per-consumer bottleneck. The replacement Kafka deployment differs from the earlier 24-trial campaign. Raw evidence is retained separately; these summaries and hashes do not replace a raw-data backup.

Both later campaigns completed with configuration restoration verified. The compressed transfer fallback recovered a large consumer evidence directory in the 1,200 messages/s trial. No additional performance trials were launched for plotting.
