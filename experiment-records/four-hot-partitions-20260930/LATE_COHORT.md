# Prespecified late production cohort

Prespecified supplementary production cohort, evaluation seconds 480–600, followed through the original drain cutoff. Whole-run results remain primary. Label as after recovery only when recovery was confirmed before 480 seconds.

Each latency describes distinct acknowledged messages produced inside the selected window, using the earliest valid completion before the original cutoff. The same cohort is used for both deadlines. Overdue unfinished records count as misses; censored deadlines and invalid clocks prevent a complete cohort miss fraction. Completion precedes commit acknowledgment.

| Condition | Run | Whole-run p99 (s) | Late-cohort p99 (s) | Late unfinished (%) | Above 0.5 s (%) | Above 1 s (%) | Recovered before window? |
|---|---:|---:|---:|---:|---:|---:|---|
| Redistribute within 3 | 1 | 84.496 | 0.187 | 0.000 | 0.136 | 0.052 | Yes |
| Redistribute within 3 | 2 | 79.281 | 0.349 | 0.000 | 0.544 | 0.048 | Yes |
| Scale to 6 + targeted redistribution | 1 | 110.190 | 2.714 | 0.000 | 5.725 | 3.796 | Yes |
| Scale to 6 + targeted redistribution | 2 | 112.939 | 0.481 | 0.000 | 0.931 | 0.126 | Yes |

The late cohort does not include messages produced earlier that were waiting during the intervention. Recovery means processing backlog at most 100 offsets for 30 consecutive valid seconds with stable ownership and nondecreasing offsets while input continues. It is a different outcome from p99 or a latency deadline. Two repetitions and shared-machine variability limit generalization.

Latency uses unadjusted producer and consumer timestamps, consistent with the primary analysis. Clock synchronization does not establish a measured bound of zero error; small cross-machine latency differences require caution.

[Whole-run outcomes and monitoring](README.md) · [Late-cohort counts and evidence hashes](late-cohort-comparison.json)

## When persistent hotspots were observed

A zero count in the final two minutes describes only that period. It does not mean persistent hotspots were absent earlier. A partition qualifies when its lag is above 10 offsets and above the partition mean plus one population standard deviation, it qualifies in at least 12 of 15 contiguous observations, and it qualifies now. These exploratory diagnostic thresholds do not by themselves establish a severe or growing backlog.

| Condition | Run | First detection (evaluation s) | Last detection (evaluation s) | Whole evaluation: nonempty / defined snapshots | Minutes 8–10: nonempty / defined snapshots |
|---|---:|---:|---:|---:|---:|
| Redistribute within 3 | 1 | 28.000 | 598.000 | 181 / 263 | 36 / 60 |
| Redistribute within 3 | 2 | 30.000 | 574.000 | 128 / 265 | 41 / 60 |
| Scale to 6 + targeted redistribution | 1 | 28.000 | 588.000 | 212 / 261 | 42 / 60 |
| Scale to 6 + targeted redistribution | 2 | 30.000 | 600.000 | 182 / 259 | 25 / 60 |

First and last detections are observed endpoints, not a claim of continuous detection between them. Ownership changes, missing observations and offset resets restart the window. Undefined observations are excluded from the defined count and are never reported as zero; complete counts are retained in the JSON. Low absolute lag can still satisfy this relative rule, so read hotspot counts together with backlog magnitude, growth and latency.
