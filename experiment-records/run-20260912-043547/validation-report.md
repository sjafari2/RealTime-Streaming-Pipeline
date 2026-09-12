# run-20260912-043547

HPA-paused three-consumer capacity calibration. All 1,440,000 admitted cohort messages completed, but no sustained processing backlog was established. The Mac slept during coordination; the same-run Prometheus export was recovered with 98.333% lag coverage. Resource request-time coverage is only 18.955%, so this run is excluded from cost comparisons. All 18 collected evidence files match the original Nautilus files. Calibration is separate from later matched comparisons.

Managed status: **complete**. Full data: `results/run-20260912-043547/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 1, and 0 SHA-256 iterations per message. The target is 12,000 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 1,440,000 |
| Completed by drain | 1,440,000 |
| Unfinished by drain | 0 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 12,000.0 |
| Unique completions / evaluation second | 12,008.408333333333 |
| Recorded completion mean (ms) | 81.3311707208554 |
| Recorded completion p99 (ms) | 1,111.7706298828125 |
| Provisional 99 ms cohort deadline-miss fraction | 0.15847986111111112 |
| Lag coverage fraction | 0.9833333333333333 |
| Covered mean returned-position lag (offsets) | 197.38135593220338 |
| Peak sampled returned-position lag (offsets) | 705.0 |
| Recorded consumer process-seconds | 1,037.3381490438478 |
| Resource request-time coverage fraction | 0.18955139549042374 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
