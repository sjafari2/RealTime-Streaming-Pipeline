# Latency for messages produced after recovery

This supplementary analysis compares redistribution within three consumers with scaling to six plus targeted redistribution. It uses the final two evaluation minutes (480–600 seconds), after confirmed recovery in all four trials. No new trials were run.

The cohort contains distinct producer-acknowledged messages produced in that half-open window. Completion means the end of application processing before commit acknowledgment. The original drain cutoff is retained, so late completions and unfinished messages remain accountable. Percentiles use nearest rank over completed cohort messages; they are not averages of rolling or consumer percentiles.

| Condition | Run | Messages | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Unfinished (%) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Redistribute within 3 | 1 | 84,000 | 87.77 | 88.07 | 133.45 | 172.44 | 0.000 |
| Redistribute within 3 | 2 | 84,000 | 88.72 | 87.43 | 133.09 | 179.77 | 0.000 |
| Scale to 6 + targeted redistribution | 1 | 84,000 | 109.51 | 107.03 | 175.73 | 211.58 | 0.000 |
| Scale to 6 + targeted redistribution | 2 | 84,000 | 111.82 | 108.17 | 180.55 | 267.50 | 0.000 |

| Condition | Run | Whole-run p99 (s) | Recovery confirmed (s from evaluation start) | Late-cohort deadline misses (%) |
|---|---:|---:|---:|---:|
| Redistribute within 3 | 1 | 80.10 | 372.0 | 31.70 |
| Redistribute within 3 | 2 | 84.16 | 396.0 | 30.90 |
| Scale to 6 + targeted redistribution | 1 | 100.81 | 236.0 | 57.66 |
| Scale to 6 + targeted redistribution | 2 | 107.24 | 252.0 | 58.97 |

The configured completion deadline is 99 ms. Unfinished work is counted separately from latency; deadline misses also retain the existing outcome definition.

## Persistent hotspots before the late window

The zero persistent-hot-partition counts in the final-two-minute analysis apply only to evaluation minutes 8–10. Persistent hotspots were observed earlier, before the intervention and during backlog reduction. The following table identifies the first and last observed nonempty persistent-hot sets; it does not assert continuous detection between those endpoints. All times start at the beginning of evaluation, after the one-minute warm-up.

| Condition | Run | First detection (s) | Last detection (s) | Minutes 8–10: snapshots with persistent hotspots / defined snapshots |
|---|---:|---:|---:|---:|
| Redistribute within 3 | 1 | 28 | 340 | 0 / 60 |
| Redistribute within 3 | 2 | 30 | 370 | 0 / 60 |
| Scale to 6 + targeted redistribution | 1 | 30 | 210 | 0 / 60 |
| Scale to 6 + targeted redistribution | 2 | 28 | 220 | 0 / 60 |

The diagnostic requires lag above 10 offsets and above the mean plus one population standard deviation, hot classification in at least 12 of the last 15 contiguous valid observations, and hot classification at the current observation. Ownership changes, missing observations and decreasing offsets reset the window; undefined values are not zeros. These are exploratory thresholds. A small but relatively high lag can still qualify, so persistent-hot counts must be interpreted together with absolute backlog, growth and latency.

## Interpretation and limits

Redistribution within three had lower recorded mean and p99 latency in both late cohorts: p99 was 172.44 and 179.77 ms, versus 211.58 and 267.50 ms with six. All four cohorts contained 84,000 acknowledged messages and had zero unfinished work. The lower late backlog observed with six therefore did not translate into lower recorded completion latency. These measurements do not identify the cause of that difference.

The whole-run p99 includes waiting before and during the intervention. This late production cohort isolates records arriving after the recovery criterion was met; it cannot erase earlier intervention costs. Both methods are supplied with the same target input, so once backlog is low, higher available capacity does not by itself require higher sustained throughput.

This window was selected after observing the results and is an exploratory diagnostic, not a prespecified primary endpoint. The comparison has two runs per method, shared-machine variability, and additional replicas whose placement was not frozen. The results do not isolate replica count from assignment, coordination, or machine effects, and finite low backlog is not proof of permanent stability.

Unadjusted cross-machine producer/completion timestamps, consistent with the original analysis. Existing clock-probe bounds do not establish millisecond synchronization; small differences require caution.

## Validation and reproduction

Every event-file hash matched the existing comparison. Full-cohort replay reproduced the saved admitted/completed/unfinished counts, mean and p99. All selected runs had confirmed recovery before 480 seconds. The existing reconciler checked partition identities, offsets, duplicate completions and final process evidence. Original evidence and summary hashes remained unchanged.

Run from the repository root:

```bash
python3 experiments/four-condition-20260929/analyze_post_recovery.py
```

[Machine-readable results and hashes](post-recovery-comparison.json) · [Whole-run comparison](README.md)
