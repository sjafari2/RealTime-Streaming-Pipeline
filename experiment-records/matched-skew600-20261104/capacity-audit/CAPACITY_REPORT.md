# Effective consumer capacity from saved matched trials

This audit adds no live experiments. It separately examines the balanced reference and the 16 validated comparison trials with the unchanged capacity-screening criteria.

| Consumer | Retrospective rate (messages/s) | Eligible-window range | Eligible 60-second intervals | Contributing trials | Separate calibration |
|---|---:|---:|---:|---:|---|
| consumer-sts-0 | 259.37 | 255.42–266.25 | 59 | 12 | No eligible interval |
| consumer-sts-1 | Not estimated | — | 0 | 0 | No eligible interval |
| consumer-sts-2 | Not estimated | — | 0 | 0 | No eligible interval |
| consumer-sts-3 | Not estimated | — | 0 | 0 | No eligible interval |
| consumer-sts-4 | 266.85 | 265.00–267.92 | 7 | 1 | No eligible interval |
| consumer-sts-5 | 293.40 | 293.07–294.02 | 6 | 1 | No eligible interval |

Completed processing attempts divided by actual scrape elapsed time, pooled over eligible intervals for each consumer. Repeated attempts count because they consume service capacity.

Existing service_window rule, unchanged: complete valid lag/ownership observations including the preceding 10 seconds, fixed ownership and process incarnation, no counter resets or ownership callbacks, counters at most 10 seconds old, six 10-second completion rates with population CV <= 0.10, and owner processing backlog always >= max(100 offsets, two seconds of observed throughput).

Retrospective effective service estimates from the comparison trials are diagnostic. They are not independently calibrated, available before action, or evidence of prospective prediction accuracy. Sampled backlog does not prove continuous busyness between observations.

Observed minimum and maximum eligible 60-second rates, not confidence limits. Multiple intervals within a run are not independent experiments.

No qualifying interval means capacity remains unestimated. Underloaded throughput is not substituted.

Completion-log cross-check: 72 eligible intervals, with a maximum count difference of 0.149% between timestamp-bounded completion events and counter increments.

Individual intervals, rejected intervals, deployment conditions and source hashes are retained in capacity-audit.json.gz.
