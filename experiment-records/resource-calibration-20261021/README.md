# Client capacity calibration — October 21, 2026

These checks reassess balanced-input capacity after the producer replacement, stricter client placement, and resource changes. They are separate from the 24 main-proposal trials. The two 700 messages/s attempts were stopped early when backlog continued to grow; neither is reported as a completed 20-minute or five-minute evaluation.

Each attempt used three producers, three fixed consumers, 60 partitions, uniform random partition choice, 2,000 application hash iterations per message, no added sleep, and no mitigation. Warm-up lasted one minute. After input stopped, consumers had two minutes to drain. Evaluation outcomes exclude warm-up messages, while monitoring includes any warm-up work still queued.

## Results at 700 messages/s

| Attempt | Consumer request per pod | Planned / actual evaluation | Processing backlog, first → last valid observation | Net backlog growth | Evaluation messages | Unfinished after drain | Completion p99 |
|---|---|---|---|---|---:|---:|---:|
| Initial allocation increase | 1.25 CPU, 640 MiB | 20 / 7.54 min | 1,355 → 10,301 | 19.79 offsets/s | 316,488 | 0 | 47.12 s |
| Higher consumer allocation | 2 CPU, 768 MiB | 5 / 2.43 min | 839 → 3,416 | 17.90 offsets/s | 101,926 | 0 | 15.73 s |

Net growth is the change in processing backlog divided by elapsed time between the first and last valid observations. Lag coverage was 99.97% and 98.90%, respectively. Plot growth uses the existing rolling calculation; it is not the same statistic as this endpoint rate. The raw offset counts include in-flight records and are not byte measurements.

Both attempts achieved approximately 700 acknowledged messages/s. Completing all evaluation messages during drain did not demonstrate that the consumers could keep up while input continued. The lower p99 of the shorter attempt is not an estimate of the resource increase's benefit: backlog had much less time to accumulate, and its initial value differed.

Producer requests remained 300m CPU and 160 MiB, with limits of 360m and 192 MiB. Consumer limits were 1.5 CPU / 768 MiB in the first attempt and 2.4 CPU / 920 MiB in the second. The same six client node names and initial partition-to-consumer assignments were verified across these two attempts. Consumer pods were recreated for the resource change; other workloads and available host capacity were not controlled.

Most backlog accumulated at `consumer-sts-1` on `k8s-chase-ci-07.calit2.optiputer.net`. It used about one CPU core in both attempts. Its cgroup counters recorded 0.016 seconds of CPU throttling in the first sampled interval and none in the second; all three consumers recorded zero throttling in the second. No restart or out-of-memory event was observed. Recorded memory usage remained below the limits. These observations do not support continuing to increase CPU quota or memory as the immediate remedy. The current application processes its CPU work sequentially; increasing its CPU limit does not itself make that work parallel.

## Relationship to the September reference

The two earlier 700 messages/s trials ran for the full 20-minute evaluation and showed bounded backlog over that observation. Their original outcomes remain unchanged. Today's calibration uses different consumer machines and a revised deployment. Important differences are:

- Required client anti-affinity replaces the historical scheduling preferences. The pods still share machines with unrelated workloads.
- The replacement producer and configuration volumes, producer names, and deployment are new.
- Producer partition sequences now use logical producer IDs instead of pod names. Seed 71 retains its intended role but does not reproduce the old hostname-seeded sequence.
- Recorded application source hashes changed. Python and the recorded Kafka, Prometheus, psutil, and YAML package versions match the historical run.
- The configured deadline is now one second instead of 99 milliseconds in the older balanced calibration. This changes deadline classification, not the measured completion timestamps.
- These two attempts ended early, unlike the completed historical observations.

The current 700 messages/s result therefore cannot be attributed solely to resources or machines. The within-day resource check controls more conditions than the historical comparison, but does not eliminate shared-node variability. See the [condition check](historical-condition-check.json), [software check](software-condition-check.json), and [within-day starting comparison](starting-condition-comparison.json).

## Evidence

| Attempt | Run ID | Summary | Configuration | Plots |
|---|---|---|---|---|
| Initial increase | `run-20261021-052534` | [Metrics](early-stop-summary.json) | [Saved configuration](early-stop-config.yaml) | [Lag](early-stop-lag.pdf), [resources and throughput](early-stop-overview.pdf) |
| Higher allocation | `run-20261021-054139` | [Metrics](retest-summary.json) | [Saved configuration](retest-config.yaml) | [Lag](retest-lag.pdf), [resources and throughput](retest-overview.pdf) |

The summaries preserve actual phase boundaries, runner interruption status, source revisions, file hashes, clock diagnostics, resources and per-consumer outcomes. The final throughput bin uses its actual observed duration when an attempt ends between ten-second boundaries. This avoids an artificial drop caused by dividing a partial interval by ten seconds.

Raw producer and consumer events, Prometheus exports and run manifests remain in separate result storage. Verified local compressed copies are described in [the first backup record](early-stop-backup-check.json) and [the retest backup record](retest-backup-check.json). Each archived file was checked by size and SHA-256. These copies are on the same computer and do not replace an independent off-site backup. Kernel clock diagnostics report synchronization status, not an independently established cross-node timing-error bound. Missing peak-memory counters remain unavailable; container peaks cover the container's lifetime and can include earlier runs and evidence transfers.

The next calibration lowers aggregate input to 600 messages/s with the higher consumer allocation. Acceptance requires sustained-input evidence, achieved input rate and measurement coverage; successful drain alone is insufficient.
