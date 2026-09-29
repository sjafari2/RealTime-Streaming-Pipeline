# Experiment progress

Updated: 29 September 2026. **52 performance trials are complete: the 48 trials summarized below and the first four trials in the new four-condition block.** Run 2 of each condition remains.

**Four responses to concentrated ownership — 700 messages/s, 13 minutes each**

Run 1 p99: keep three **366.64 s**, redistribution **80.10 s**, scaling plus targeted redistribution **100.81 s**, and normal Kafka scaling **71.41 s**. The baseline left **34.64%** unfinished; all three interventions finished every evaluation message and met the specified recovery criterion while input continued. These are the first trials per condition; Run 2 is still in progress. The long offset-initialization monitoring gap was diagnosed and corrected before this block. A separate local-computer sleep interrupted baseline collection; its historical lag/CPU/memory data were recovered without rerunning traffic, while missing resource-request samples remain flagged.

[Current progress and evidence](experiment-records/four-condition-20260929/README.md)

**Redistribution without scaling — 700 messages/s, 8 minutes each**

Run 1: completion p99 **226.34 → 103.76 seconds**; unfinished **30.18% → 0.00%**.

Run 2: completion p99 **227.13 → 91.38 seconds**; unfinished **30.48% → 0.00%**.

Each arrow compares a separate keep-three trial with a redistribution trial. Both start with all twelve hot partitions on Consumer 2. The intervention gives each of the same three consumers four hot partitions; no replicas are added. A separate nonempty handoff check completed all 167,986 acknowledged messages before the four performance trials. Interruption, outstanding-work accumulation and recovery were recorded using definitions fixed before this block.

[Results and plots](experiment-records/c2-redistribution-20260928/README.md) · [Intervention costs](experiment-records/c2-redistribution-20260928/INTERVENTION_COST.md)

**Scaling from Consumer 2 concentration — 700 messages/s, 8 minutes each**

Run 1: completion p99 **231.58 → 110.70 seconds**; unfinished evaluation messages **31.76% → 0.00%**.

Run 2: completion p99 **229.28 → 109.47 seconds**; unfinished evaluation messages **31.04% → 0.00%**.

Each arrow compares a separate keep-three trial with a scale-to-six trial that also redistributes the hot partitions. Both start with all twelve hot partitions on Consumer 2. These results support the combined intervention; they do not show that extra consumers were necessary. Two technical handoff checks and one failed empty preparation are excluded from the performance count.

[Results and all plots](experiment-records/c2-scaling-20260927/README.md) · [Preparation checks](experiment-records/c2-scaling-20260927/PREPARATION.md)

**Latest: hot-partition ownership at 700 messages/s — 8 minutes each**

All **210,000 evaluation messages** finished in the distributed layout and when all twelve hot partitions were placed on Consumer 0 or Consumer 1. Their p99 values were **0.193, 0.249 and 0.248 seconds**, respectively; peak lag remained between **58 and 141 offsets**.

With all twelve hot partitions on **Consumer 2**, backlog grew by approximately **288 offsets/s**, peak lag reached **102,475**, and **32.29%** of evaluation messages remained unfinished after the drain. Completed-message p99 was **233.86 seconds**. The same aggregate input and skew produced different outcomes depending on the observed processing capacity of the selected owner. These are one-off starting-layout calibrations, with no scaling or live redistribution.

[Combined ownership results and plots](experiment-records/hot-ownership-consumers-20260927/README.md)

**Balanced stability — 23 minutes each, three consumers throughout**

Two trials each at **600, 700 and 800 messages/s** showed low lag. At **900**, backlog grew during production although every evaluation message finished during the drain. At **1,200 and 1,500**, backlog grew and some evaluation messages remained unfinished. These results describe the observed intervals and shared machines; they do not guarantee permanent stability.

[All 12 stability results and plots](experiment-records/stability-fixed3-20260921/COMPLETED_RESULTS.md)

**Original scaling comparisons**

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

**Additional preparation and calibration**

We completed two initial collection checks and separate capacity/pressure diagnostics. Six later startup checks stopped because partition ownership did not match; they sent no experiment messages and added no performance results.

**Single-partition, starting ownership matched — 7 minutes total**

Both pairs started with matching partition ownership and original pods/machines. Scaling **helped in the first pair but worsened the second**. Unfinished messages changed from **39.49% to 25.10%**, then from **39.16% to 61.29%**. Completion p99 changed from **228.44 to 192.79 seconds**, then **228.76 to 294.13 seconds**. In the second scaling run, the hot partition moved to a consumer with a longer recorded application-task time. More consumers did not reliably resolve this single-partition workload.

**80/20 across 12 partitions — 7 minutes total**

With **80% of traffic spread across 12 of 60 partitions**, scaling lowered recorded completion p99 in both pairs: **121.15 to 49.37 seconds**, then **115.68 to 50.95 seconds**. Scaling finished every evaluation message in both runs; keep-three left **0.53%** unfinished in the first and **0%** in the second. Scaling used more requested CPU; monitoring coverage and delayed capacity remain limitations.


**Historical preparation records**

The September 15–17 storage and Kafka I/O failures occurred before the completed stability series above. Those failed preparations are documented separately and are not performance trials.

[Storage recovery](experiment-records/producer-storage-recovery-20260915/README.md) · [September 17 preparation](experiment-records/stability-preflight-20260917/README.md)

[Repeated results and plots](experiment-records/repetitions-20260913/README.md) · [Why the second single-partition result differed](experiment-records/repetitions-20260913/single-partition/ownership-review.md) · [Earlier detailed results](experiment-records/README.md)

[Proposal update and one-page summary](experiment-records/proposal-progress-20260913/README.md)
