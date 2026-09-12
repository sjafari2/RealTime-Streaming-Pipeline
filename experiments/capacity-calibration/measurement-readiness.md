# Measurement readiness — 12 September 2026

These diagnostics support pressure-test preparation. They do not change the
existing run summaries or establish capacity, mitigation benefit or clock-validated
deadline performance.

## Existing run evidence

| Run | Distribution | Valid lag coverage | Peak returned-position lag |
|---|---|---:|---:|
| `run-20260911-214924` | Static 80/20 skew | 87.33% | 765 offsets |
| `run-20260911-234758` | Balanced | 94.00% | 734 offsets |

Both used a target of 9,000 messages/s total, six consumers and no mitigation.
Both completed all admitted messages by the end of drain. Neither alone establishes
sustained overload. These peaks are the existing lag-summary values, not a new
processing-backlog capacity estimate. See the corresponding `experiment-records/`
directories for latency, deadline, counts and validity limitations.

## Why lag observations were rejected

The saved audit identifies 12 invalid query samples in the skewed run and five in
the balanced run. At startup, some offset values were unavailable/nonfinite or had
an invalid flag. Later, some consumer observation timestamps were slightly ahead
of their monitoring query timestamps: roughly 7–81 ms in the skewed run and 7–79 ms
in the balanced run. The balanced run also has this timing issue at about 4 seconds.
Reason counts in `diagnostics/lag-audit.json` count affected partition observations;
they overlap and must not be interpreted as independent failed query counts.

`analyze_lag.py` rejects negative observation age. These gaps do not demonstrate
ongoing broker-query timeouts. Historical maximum consumer scrape durations were
about 188–292 ms, making scrape alignment a plausible contributor. The exported
data does not uniquely separate scrape timing, concurrent metric updates and clock
offset. Do not loosen the freshness rule or fill missing samples with zero just to
raise coverage. Keep the historical coverage figures. The new warm-up separates
startup from evaluation, but improved coverage must still be measured.

## Current clocks

At 2026-09-12 07:47:17 UTC, read-only checks succeeded in all three producer and six
consumer pods. Each kernel reported state 0, no unsynchronized/error flags, and a
reported maximum-error estimate between 1.171 and 4.216 ms. No container had
`chronyc`, and the queried Prometheus instance exposed no node timex/time series.

These are current kernel reports, not independent cross-node error bounds or
retrospective validation of the earlier runs. Their reported zero offset does not
mean clocks are exactly equal. Repeat at run boundaries and obtain clock-source
tracking when possible. The utility uses `adjtimex` with modes zero, which only
queries status; see the [Linux API documentation](https://man7.org/linux/man-pages/man2/adjtimex.2.html).
It creates no debug pods, mounts no hosts, installs no cluster packages and changes
no clock parameters. Full placements, resources and samples are retained in
`diagnostics/clock-diagnostics.json`.

## Placement and monitoring

All three Kafka broker pods and `consumer-sts-0` currently share
`patternlab.calit2.optiputer.net`. This is a possible source of shared contention,
not evidence that contention caused either pilot result. Observe that node and
separate broker/consumer resource behavior when applying pressure.

The Prometheus job inventory contains nine application target series and only one
broker target series. This counts series, not successful targets. Verify coverage
of all three brokers before claiming the broker tier has spare capacity; one target
does not establish that all brokers were observed. Read-only `kubectl top pods
--containers` works, but an idle CPU snapshot cannot establish performance under
load, and container memory differs from the application process RSS metric.

The original local port-forward was unavailable. A temporary runner-managed
Prometheus forward succeeded and was closed after the read. No authentication
failure or monitoring configuration change was needed for these diagnostics.

## Before interpreting a pressure run

Confirm actual admitted traffic, valid multi-partition backlog growth and the
limiting stage. Retain per-run source/configuration identity and complete evidence.
If clock evidence remains insufficient, describe latency as recorded latency and
keep that limitation visible. If monitoring coverage or broker attribution is
insufficient, preserve the run as diagnostic evidence rather than presenting it
as a validated capacity/scaling comparison.
