# run-20260912-100035

Frozen low-input pilot, pair A2, scheduled action scale. HPA paused; initial replicas checked. This is a preliminary action comparison, not a full adaptive controller evaluation. Interpret latency with unfinished outcomes, achieved admission, coverage, deployment/ownership events and common-horizon resource requests.

Managed status: **complete**. Full data: `results/run-20260912-100035/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 52, and 2000 SHA-256 iterations per message. The target is 600 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 72,000 |
| Completed by drain | 72,000 |
| Unfinished by drain | 0 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 600 |
| Unique completions / evaluation second | 600 |
| Recorded completion mean (ms) | 162.6122 |
| Recorded completion p99 (ms) | 899.6537 |
| Provisional 99 ms cohort deadline-miss fraction | 0.6733 |
| Lag coverage fraction | 0.9333 |
| Covered mean returned-position lag (offsets) | 63.2946 |
| Peak sampled returned-position lag (offsets) | 147 |
| Recorded consumer process-seconds | 1,536.7322 |
| Resource request-time coverage fraction | 0.9338 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
