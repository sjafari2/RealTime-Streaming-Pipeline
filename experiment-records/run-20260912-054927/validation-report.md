# run-20260912-054927

Qualified CPU pressure calibration with coherent lag snapshots. Exactly 180,000 evaluation admissions; 3,103 remained unfinished by drain (1.724%). Backlog grew by 13,187 and 12,943.5 offsets in its two evaluation minutes. Lag coverage was 98.333%, above the declared 90% screen; no outcome validity failures were detected. All 18 evidence files match Nautilus. This calibration selects the workload for the separately frozen matched comparisons; it is not itself a scaling result.

Managed status: **complete**. Full data: `results/run-20260912-054927/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 11, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 180,000 |
| Completed by drain | 176,897 |
| Unfinished by drain | 3,103 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 1,500 |
| Unique completions / evaluation second | 1,241.8583 |
| Recorded completion mean (ms) | 31,674.6387 |
| Recorded completion p99 (ms) | 123,677.4955 |
| Provisional 99 ms cohort deadline-miss fraction | 0.8014 |
| Lag coverage fraction | 0.9833 |
| Covered mean returned-position lag (offsets) | 29,915.4407 |
| Peak sampled returned-position lag (offsets) | 45,681 |
| Recorded consumer process-seconds | 1,029.9454 |
| Resource request-time coverage fraction | 0.935 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
