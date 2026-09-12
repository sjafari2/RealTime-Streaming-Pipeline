# run-20260912-082113

Frozen short pilot, pair S2, scheduled action none. HPA paused; initial replicas checked. This is a preliminary action comparison, not a full adaptive controller evaluation. Interpret latency with unfinished outcomes, achieved admission, coverage, deployment/ownership events and common-horizon resource requests.

Managed status: **complete**. Full data: `results/run-20260912-082113/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 32, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 180,000 |
| Completed by drain | 180,000 |
| Unfinished by drain | 0 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 1,500 |
| Unique completions / evaluation second | 1,252.75 |
| Recorded completion mean (ms) | 31,049.7703 |
| Recorded completion p99 (ms) | 115,817.1558 |
| Provisional 99 ms cohort deadline-miss fraction | 0.8134 |
| Lag coverage fraction | 0.9833 |
| Covered mean returned-position lag (offsets) | 29,041.6525 |
| Peak sampled returned-position lag (offsets) | 43,525 |
| Recorded consumer process-seconds | 1,030.8018 |
| Resource request-time coverage fraction | 0.9326 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
