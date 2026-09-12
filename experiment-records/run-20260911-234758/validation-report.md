# Balanced Nautilus validation

Run `run-20260911-234758` completed successfully using source revision `46c5d4aa22d6a568c9cad01ecc73180b49672ba8`.
This was one short balanced run without mitigation. The workload, bounded drain,
Prometheus export and exact outcome evaluation all completed successfully. The
initial consumer-volume copy failed with `unexpected EOF`; collection recovered by
copying each consumer pod's retained evidence directory separately. The workload
was not repeated.

## Configuration and timing

Three producers targeted 3,000 messages/second each (9,000 total), with six consumers,
one topic and 60 partitions. The payload setting was 100 bytes, workload seed 1,
`APP_DELAY_MS=0`, and the recorded consumer task used no SHA-256 iterations. There
was no intervention plan. The shared configuration was updated before launch and
the runner used its frozen copy throughout this experiment.

Production/evaluation: **2026-09-11 23:49:05 MDT** to
**2026-09-11 23:54:05 MDT**. Warm-up: 0 seconds; production:
300 seconds; drain: 60 seconds, ending **2026-09-11 23:55:05 MDT**.
The final scrape hold was 15 seconds. Preparation, transfer and analysis are outside
the five-minute production interval.

## Whole-run outcomes and the previous run

| Metric | Balanced: this run | Previous skewed run |
|---|---:|---:|
| Distinct admitted messages | 2,699,808 | 2,699,197 |
| Completed by drain | 2,699,808 | 2,699,197 |
| Unfinished by drain | 0 | 0 |
| Mean completion latency | 52.632 ms | 46.958 ms |
| Completion p95 | 301.279 ms | 254.503 ms |
| Completion p99 | 764.188 ms | 662.856 ms |
| Recorded misses above 99 ms | 254,589 (9.4299%) | 238,485 (8.8354%) |
| Admitted messages/evaluation second | 8999.360 | 8997.323 |
| Unique completions/evaluation second | 8998.810 | 8996.923 |

The previous run was `run-20260911-214924`, with the same replica counts, rate, payload,
seed and short timing but a configured 80% of messages directed to 12 hot partitions.
This table describes two observed runs; it does not establish a repeatable or causal
performance difference on the shared cluster.

All 60 partitions received between **44,609 and 45,466** admitted
messages (mean 44996.80; population coefficient of variation
0.4080%). This is the observed partition balance,
separate from consumer capacity or queue stability.

Completion is measured after the configured application work and before commit
acknowledgment. Mean uses the sum of valid distinct-message completion latencies
divided by their count. Percentiles use nearest rank over that same cohort, including
messages completed during drain. Unfinished messages are reported separately.
Rolling 30-second Grafana histogram values use a different window and estimator.

## Verification and limitations

All 9 processes finished cleanly, with no evidence drops or writer errors.
All 27 copied evidence files match the retained Nautilus files by SHA-256.
Process startup hashes match the recorded runtime source, and every process used the
same frozen configuration hash. The exporter retained 1,671 Prometheus
series with no missing incarnations. Outcome validity checks passed.
Duplicate completions: 0; failed/cancelled sends:
0; unresolved sends: 0.
Per-partition admitted/completed/deadline totals reconcile with pipeline totals.

Lag integration covers **282/300 seconds
(94.0000%)**. Covered-interval mean lag is
136.043 offsets; peak sampled lag is 734.
Exact evaluation-boundary lag is unavailable because no valid sample falls exactly
on that boundary. Missing samples are not filled with zero.
Consumer process lifetimes total 2489.958 seconds;
sampled resource-request accounting covers 92.1428% of its
observation span. These are process duration and requested-resource declarations,
not actual CPU consumption or whole-cluster cost. Recovery is not applicable without
an intervention.

Clock-probe round trips ranged from 622 to 753 ms. These probes do not
establish cross-node clock accuracy for a strict 99 ms deadline claim. The latency and
deadline figures above are recorded timestamp measurements. Clock calibration and
coverage investigation remain necessary for thesis conclusions.

This is a monitoring validation, not the proposal's longer experiment protocol,
capacity calibration, formal Case A/B acceptance or evidence that scaling would help.
The live preflight checked dependencies and source across all nine pods. Producer
and consumer processing code did not change. After the workload, the local collector
was updated to copy each saved pod directory separately and retry API stream errors;
this applies to both single and repeated runs. The maintenance commit is
`0975d4b26bd49517b6fe23894d49373199cb399c` and all 61 tests passed. That
maintenance revision is separate from the original workload revision.

The existing Grafana dashboard was not edited. Its screenshots and a separate
run-filtered PNG/SVG plot set, CSV/JSON plot data and exact PromQL queries are saved
under [plots](plots/README.md). The rolling p99 plot peaks near 1,438 ms; it uses a
different window and estimator from the whole-run p99 above. The figures do not
remove the clock/coverage limitations.

## Saved evidence

Full local results: `/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-234758` (about 2.27 GB before this report).
Original evidence remains on each role's Nautilus volume under `evidence/run-20260911-234758`.
No old topics or evidence were deleted. No new cloud storage backup was configured.

This Git record retains the frozen configuration, outcome/lag/execution summaries,
source revision and evidence checksums. It excludes raw per-message events and full
Prometheus exports. See [outcomes](outcome-summary.json), [lag](lag-summary.json),
[execution](execution-summary.json), [configuration](pipeline-configmap.yaml),
[source identity](source-at-run-start.json) and [integrity](evidence-integrity.json).
