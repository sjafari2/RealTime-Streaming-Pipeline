# Experiment progress

Updated: 15 September 2026. **24 comparison runs completed. Both controlled skew comparisons now have two runs per treatment.**

The first four conditions each had four runs: twice with three consumers and twice with scaling from three to six. Later controlled comparisons added eight runs.

**Balanced pressure — 12 minutes total**

The three-consumer runs left **11.38% and 9.62%** unfinished. Both scaling runs finished all evaluation messages by the drain cutoff. Recorded completion p99 decreased from **266–292 seconds to 79–96 seconds**.

**Short balanced pressure — 5 minutes total**

Scaling lowered p99 in both repetitions. However, one three-consumer run also completed every evaluation message, showing that unfinished outcomes varied between repetitions. Gaps in monitoring limit the backlog comparison.

**Concentrated input — 7 minutes total**

With **80% of traffic directed to one partition**, backlog continued growing after scaling. Approximately **44–46% remained unfinished** in the scaling runs. The runs started with different partition owners and machines. A later repeat checked matching starting conditions (below).

**Lower target rate (600 messages/s) — 5 minutes total**

All four runs finished every evaluation message. Scaling increased requested resources, while recorded p99 was slightly higher in both scaling runs.

All durations include one minute of warm-up and two minutes of drain; the remaining time is evaluation production. Unfinished means still incomplete at that cutoff; p99 includes only completed evaluation messages. These are preliminary results.

**What else is done?**

We completed two initial collection checks and separate capacity/pressure diagnostics. Six later startup checks stopped because partition ownership did not match; they sent no experiment messages and added no performance results.

**Single-partition, starting ownership matched — 7 minutes total**

Both pairs started with matching partition ownership and original pods/machines. Scaling **helped in the first pair but worsened the second**. Unfinished messages changed from **39.49% to 25.10%**, then from **39.16% to 61.29%**. Completion p99 changed from **228.44 to 192.79 seconds**, then **228.76 to 294.13 seconds**. In the second scaling run, the hot partition moved to a consumer with a longer recorded application-task time. More consumers did not reliably resolve this single-partition workload.

**80/20 across 12 partitions — 7 minutes total**

With **80% of traffic spread across 12 of 60 partitions**, scaling lowered recorded completion p99 in both pairs: **121.15 to 49.37 seconds**, then **115.68 to 50.95 seconds**. Scaling finished every evaluation message in both runs; keep-three left **0.53%** unfinished in the first and **0%** in the second. Scaling used more requested CPU; monitoring coverage and delayed capacity remain limitations.


**What is next?**

Six balanced stability trials are planned: lower target rate with three consumers, pressure with three consumers, and pressure with scaling to six, each twice. Each lasts 23 minutes total (1 warm-up + 20 evaluation + 2 drain). They test whether backlog settles while production continues. **None has started:** the configuration storage has now been replaced successfully, and a fresh consumer started. Producer data access, the execution wrapper and live calibration/checks remain pending.

[Latest recovery status](experiment-records/config-storage-replacement-20260915/README.md)

[Repeated results and plots](experiment-records/repetitions-20260913/README.md) · [Why the second single-partition result differed](experiment-records/repetitions-20260913/single-partition/ownership-review.md) · [Earlier detailed results](experiment-records/README.md)

[Proposal update and one-page summary](experiment-records/proposal-progress-20260913/README.md)
