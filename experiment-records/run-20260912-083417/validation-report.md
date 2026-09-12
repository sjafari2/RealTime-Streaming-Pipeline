# run-20260912-083417

Dedicated one-partition/one-consumer capacity calibration at 1200 messages/s and 2000 SHA-256 iterations/message. Interpret only after the separately recorded qualification checks; this is one observed node and interval, not a universal service-capacity constant.

Managed status: **complete**. Full data: `results/run-20260912-083417/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 1 initial consumers, 1 partitions, balanced input, seed 40, and 2000 SHA-256 iterations per message. The target is 1,200 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 144,000 |
| Completed by drain | 119,313 |
| Unfinished by drain | 24,687 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 1,200 |
| Unique completions / evaluation second | 639.1833 |
| Recorded completion mean (ms) | 97,635.9994 |
| Recorded completion p99 (ms) | 139,702.0018 |
| Provisional 99 ms cohort deadline-miss fraction | 1 |
| Lag coverage fraction | 0.9833 |
| Covered mean returned-position lag (offsets) | 67,242.6441 |
| Peak sampled returned-position lag (offsets) | 98,837 |
| Recorded consumer process-seconds | 332.7169 |
| Resource request-time coverage fraction | 0.9486 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
