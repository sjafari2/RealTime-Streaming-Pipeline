# run-20260912-053411

Consumer-pressure calibration: backlog grew in both evaluation minutes at the achieved 1,500 messages/s, and 3,141 of 180,000 evaluation admissions remained unfinished by drain (1.745%). No outcome validity failures or duplicate cohort completions were detected. Lag integration coverage was 85%, below the predeclared 90% screen, because a scrape could mix fields from different lag updates. Preserve this limitation; this is not a qualified matched comparison or a scaling result. The monitoring correction is tested and a separate calibration repeat follows.

Managed status: **complete**. Full data: `results/run-20260912-053411/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 11, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

| Measure | Value |
| --- | ---: |
| Distinct evaluation admissions | 180,000 |
| Completed by drain | 176,859 |
| Unfinished by drain | 3,141 |
| Duplicate cohort completion attempts | 0 |
| Admissions / evaluation second | 1,500.0 |
| Unique completions / evaluation second | 1,241.1 |
| Recorded completion mean (ms) | 31,634.916772850982 |
| Recorded completion p99 (ms) | 123,665.26412963867 |
| Provisional 99 ms cohort deadline-miss fraction | 0.8005 |
| Lag coverage fraction | 0.85 |
| Covered mean returned-position lag (offsets) | 30,330.63725490196 |
| Peak sampled returned-position lag (offsets) | 44,846.0 |
| Recorded consumer process-seconds | 1,029.5820240002358 |
| Resource request-time coverage fraction | 0.9349860276995156 |

Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.

Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.

Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.
