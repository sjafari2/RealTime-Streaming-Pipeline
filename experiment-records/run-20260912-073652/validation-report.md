# run-20260912-073652

Frozen sustained pilot, pair L2, scheduled action none. HPA paused; initial replicas checked. This is a preliminary action comparison, not a full adaptive controller evaluation. Interpret latency with unfinished outcomes, achieved admission, coverage, deployment/ownership events and common-horizon resource requests.

Managed status: **complete**. Full data: `results/run-20260912-073652/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 22, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 600 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 810,000 |
| Completed by drain | 732,054 |
| Unfinished by drain | 77,946 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 1,500 |
| Unique completions / evaluation second | 1,263.0722 |
| Recorded completion mean (ms) | 54,722.7557 |
| Recorded completion p99 (ms) | 266,315.6538 |
| Provisional 99 ms cohort deadline-miss fraction | 0.815 |
| Lag coverage fraction | 0.9963 |
| Covered mean returned-position lag (offsets) | 79,049.6171 |
| Peak sampled returned-position lag (offsets) | 142,274 |
| Recorded consumer process-seconds | 2,292.6393 |
| Resource request-time coverage fraction | 0.9676 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
