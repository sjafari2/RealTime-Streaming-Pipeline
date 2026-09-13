# Experiment progress

Updated: 12 September 2026. **16 comparison runs completed. The next experiment is waiting for your confirmation.**

For each condition below, we ran twice with three consumers and twice with scaling from three to six: four runs per condition.

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

**What is next?**

After your confirmation: four startup rehearsals without messages. If all pass, repeat the concentrated-input comparison once with scaling and once with three consumers, using the same starting machines and partition ownership. This revised procedure has not been deployed or run.

[Detailed results, run records and saved plots](experiment-records/README.md)
