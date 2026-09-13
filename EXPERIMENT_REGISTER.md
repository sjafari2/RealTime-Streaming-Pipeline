# Experiment progress

Updated: 13 September 2026. **20 comparison runs completed. The 80/20 scaling comparison is now finished.**

The first four conditions each had four runs: twice with three consumers and twice with scaling from three to six. Two later comparisons added four runs.

**Balanced pressure — 10 minutes**

The three-consumer runs left **11.38% and 9.62%** unfinished. Both scaling runs finished all evaluation messages by the drain cutoff. Recorded completion p99 decreased from **266–292 seconds to 79–96 seconds**.

**Short balanced pressure — 3 minutes**

Scaling lowered p99 in both repetitions. However, one three-consumer run also completed every evaluation message, showing that unfinished outcomes varied between repetitions. Gaps in monitoring limit the backlog comparison.

**Concentrated input — 5 minutes**

With **80% of traffic directed to one partition**, backlog continued growing after scaling. Approximately **44–46% remained unfinished** in the scaling runs. The runs started with different partition owners and machines. A later repeat checked matching starting conditions (below).

**Lower input — 3 minutes**

All four runs finished every evaluation message. Scaling increased requested resources, while recorded p99 was slightly higher in both scaling runs.

The durations above include one minute of warm-up, followed by an additional two-minute drain. Unfinished means still incomplete at that cutoff; p99 includes only completed evaluation messages. These are preliminary results.

**What else is done?**

We completed two initial collection checks and separate capacity/pressure diagnostics. Six later startup checks stopped because partition ownership did not match; they sent no experiment messages and added no performance results.

**Single-partition controlled repeat — 5 minutes**

Both runs started with the same partition ownership and original pods/machines. Keeping three consumers left **39.49% unfinished**, versus **25.10% after scaling**. Recorded completion p99 was **228.44 versus 192.79 seconds**. Scaling used more requested CPU and backlog still grew. This is one comparison; shared-machine conditions remain a limitation.

All four empty startup rehearsals passed. Both real runs finished, evidence checks passed, and original settings were restored.

**80/20 across 12 partitions — 5 minutes**

With **80% of traffic spread across 12 of 60 partitions**, keeping three consumers left **0.53% unfinished**, while scaling finished all evaluation messages. Recorded completion p99 was **121.15 seconds versus 49.37 seconds**. Scaling requested more CPU (**36.00 versus 65.16 core-minutes**). Monitoring gaps limit aggregate backlog comparisons. This is one comparison.

**What is next?**

The 80/20 review is complete: scaling helped in this pair, using more requested CPU. The transition check also passed: later commits covered every failed offset, and monitoring gaps matched partition transfers. Next, repeat the same workload with keep-three first and scaling second. This is a recommendation; no additional run has started.

[Short result review and next experiment](experiment-records/8020-comparison-20260913/result-review.md)

[80/20 result and plots](experiment-records/8020-comparison-20260913/README.md) · [Single-partition repeat](experiment-records/static-startup-executed-20260912/README.md) · [Earlier detailed results](experiment-records/README.md)
