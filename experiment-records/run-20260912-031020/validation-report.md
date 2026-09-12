# Balanced capacity calibration — run-20260912-031020

This short Nautilus trial maintained low processing backlog at a target of 12,000
messages/s with six consumers. It does not demonstrate sustained overload or a
benefit from scaling. Calibration is reported separately from policy comparisons.

Three producers, six consumers and 60 partitions used balanced input, a 100-byte
payload setting, seed 1, and no added sleep or SHA-256 iterations. Production lasted
180 seconds; evaluation excluded the first 60 seconds, with a 120-second drain.
The runtime and analysis were loaded from commit 80e958d before the later compression
and cache update. No application source was changed during this trial.

| Reconciled result | Value |
| --- | ---: |
| Distinct evaluation-cohort admissions | 1,440,001 |
| Cohort completed by drain | 1,440,001 |
| Unfinished cohort messages | 0 |
| Duplicate cohort completion attempts | 0 |
| Cohort admissions per evaluation second | 12,000.0083 |
| Unique completions during evaluation per second | 12,000.1500 |
| Recorded completion mean | 60.3413 ms |
| Recorded completion p50 / p95 / p99 | 13.7510 / 375.3862 / 911.6585 ms |
| Recorded cohort delays above provisional 99 ms | 150,244 (10.4336%) |
| Lag integration coverage | 118 / 120 s (98.3333%) |
| Covered mean returned-position lag | 227.7458 offsets |
| Peak sampled returned-position lag | 975 offsets |
| Consumer process lifetime sum | 2,110.3374 process-seconds |
| Consumer request-time coverage | 92.1340% |

Whole-cohort latency follows distinct acknowledged messages produced during
**evaluation** to earliest valid processing completion through drain, before commit
acknowledgment. Rolling plot rates/quantiles have different populations. There were
no reported failed/cancelled/unresolved sends, evidence drops/errors, conflicting
identities/offsets, invalid latency observations or within-epoch ordering failures.
All nine final process snapshots were clean and all 27 evidence files matched the
original Nautilus files by SHA-256. One producer copy required a retry; the verified
data comes from the same trial, not a rerun.

The live 5-second guard had 24 valid evaluation samples. Median-boundary backlog
changes in its two 60-second windows were -1.5 and +324.5 offsets, below the declared
1,500-offset growth screen. The archived full trace shows spikes that receded, not
persistent growth. Clock-discipline self-reports were synchronized, but are not an
independently established cross-node accuracy bound; the 99 ms figures are recorded
timestamp comparisons, not an application SLA validation.

## Controller and infrastructure qualifications

A consumer HPA with a minimum of six replicas was discovered during the subsequent
attempt to calibrate three consumers. Its settings were not captured at this run's
start. All retained resource-history samples in this run and the two earlier pilots
show six consumers; no replica change is observed. This trial is therefore a
qualified six-consumer reference, **not an HPA-disabled control**. The later controlled
trials pause both HPA scaling directions and use compatible bounds.

The attempted Prometheus discovery deployment was rejected by Nautilus because of
low account resource utilization. The original monitoring configuration was restored
on its existing PVC. The six static consumer targets remained at 5-second scrapes;
broker JMX scrapes were incomplete. Separate container-resource samples are retained.
Consumer 0 shared a physical node with all three brokers. Placement and differing
process CPU observations prevent a broad claim of identical consumer capacity.

## Retained files and reproducibility

The complete run is in `results/run-20260912-031020/` in the active local code folder,
with the original evidence also retained on the Nautilus producer/consumer PVCs.
Local message logs were losslessly compressed from 1,695,952,735 to 119,166,381 bytes.
The decompressed bytes match the original SHA-256 values; no messages were sampled
or discarded. `compressed-evidence.json` maps original and compressed paths/hashes.
The analysis readers accept either representation. Git stores these small records
and figures; it is not the raw-data backup.

Redraw the archived figures offline with `python3 plots/draw_run.py plots`. The
query JSON files, lag summary and plotting environment are retained with the plots.
The CSV contains the actual plotted processing-backlog samples and validity flag.
