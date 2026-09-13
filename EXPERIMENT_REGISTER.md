# Experiment progress

Updated: 13 September 2026. **18 comparison runs completed. The latest controlled repeat finished successfully.**

The first four conditions each had four runs: twice with three consumers and twice with scaling from three to six. The latest repeat added two runs.

**Balanced pressure — 10 minutes**

The three-consumer runs left **11.38% and 9.62%** unfinished. Both scaling runs finished all evaluation messages by the drain cutoff. Recorded completion p99 decreased from **266–292 seconds to 79–96 seconds**.

**Short balanced pressure — 3 minutes**

Scaling lowered p99 in both repetitions. However, one three-consumer run also completed every evaluation message, showing that unfinished outcomes varied between repetitions. Gaps in monitoring limit the backlog comparison.

**Concentrated input — 5 minutes**

With **80% of traffic directed to one partition**, backlog continued growing after scaling. Approximately **44–46% remained unfinished** in the scaling runs. The runs started with different partition owners and machines, so we need a better-controlled repeat before attributing the differences to scaling.

**Lower input — 3 minutes**

All four runs finished every evaluation message. Scaling increased requested resources, while recorded p99 was slightly higher in both scaling runs.

The durations above include one minute of warm-up, followed by an additional two-minute drain. Unfinished means still incomplete at that cutoff; p99 includes only completed evaluation messages. These are preliminary results.

**What else is done?**

We completed two initial collection checks and separate capacity/pressure diagnostics. Six later startup checks stopped because partition ownership did not match; they sent no experiment messages and added no performance results.

**Latest controlled repeat — 5 minutes**

Both runs started with the same partition ownership and original pods/machines. Keeping three consumers left **39.49% unfinished**, versus **25.10% after scaling**. Recorded completion p99 was **228.44 versus 192.79 seconds**. Scaling used more requested CPU and backlog still grew. This is one comparison; shared-machine conditions remain a limitation.

All four empty startup rehearsals passed. Both real runs finished, evidence checks passed, and original settings were restored.

**What is next?**

Review this new result before choosing another experiment. No additional run has started.

[Latest result and plots](experiment-records/static-startup-executed-20260912/README.md) · [Earlier detailed results](experiment-records/README.md)
