# Nautilus validation run

Run `run-20260911-214924` completed its scheduled workload and passed the available identity and evidence checks. The initial large-file transfer ended with `unexpected EOF`; the same retained evidence was then collected successfully after fixing the transfer settings. No replacement workload was needed.

## Configuration and timing

There were three producers, six consumers, one topic and 60 partitions. The target was 3,000 messages/second per producer, or 9,000 across the pipeline. The frozen configuration used a 100-byte payload setting, workload seed 1, hot-subset probability 0.8, hot-partition fraction 0.2, APP_DELAY_MS=0, ACKS=all and idempotence enabled. These are this pilot's settings, not calibrated thesis-wide values.

Production and evaluation ran from 21:50:26.990 to 21:55:26.990 MDT on 11 September 2026. There was no warm-up. The drain ended at 21:56:26.990 MDT. This was an ordinary run with no scheduled scaling or reassignment.

## Message outcomes

| Metric | Result |
|---|---:|
| Distinct admitted messages | 2,699,197 |
| Completed by drain | 2,699,197 |
| Unfinished by drain | 0 |
| Duplicate completion attempts in the admitted cohort | 0 |
| Mean completion latency | 46.958 ms |
| Completion p50 | 15.019 ms |
| Completion p95 | 254.503 ms |
| Completion p99 | 662.856 ms |
| Recorded deadline misses above 99 ms | 238,485 (8.8354%) |
| Admitted messages per evaluation second | 8997.323 |
| Unique completions per evaluation second | 8996.923 |

Completion is after the configured synthetic work and before commit acknowledgment. Percentiles use nearest rank among distinct admitted messages that completed by the drain bound. Whole-run results are separate from rolling Grafana attempt metrics. All admitted messages finished, but the recorded 99 ms objective was not met.

There were no failed/cancelled sends, unresolved sends, invalid/negative latency observations, conflicting identities, conflicting physical offsets or within-epoch ordering violations. The checks do not establish durable application effects or exactly-once behavior at a downstream sink.

## Coverage and interpretation

Lag integration covered 262 of 300 seconds (87.33%). Of 150 exported evaluation samples, 138 were valid; invalid partition observations break adjacent intervals. Mean lag over covered intervals was 168.286 offsets and peak sampled lag was 765 offsets. The exact evaluation-boundary lag is unavailable. Missing observations were not replaced with zero.

Consumer process lifetimes totaled 2467.901 process-seconds, including preparation and drain and excluding the final scrape hold. Sampled resource-request accounting covered 92.98% of its observation span. It measures consumer-container declarations on covered intervals, not actual CPU usage or whole-cluster cost. Recovery is not applicable because this run had no intervention plan.

The kubectl clock probes have round-trip uncertainty of roughly 603-659 ms. They do not establish the relative node-clock accuracy needed for a strict 99 ms claim. The latency figures are recorded timestamp differences; clock calibration and lag-coverage investigation remain necessary before treating this pilot as a final thesis comparison.

## Evidence and code verification

All nine processes saved clean final snapshots with no evidence drops or writer errors. Prometheus export contains 1,671 series with no missing process incarnations. All 27 copied evidence files match the retained Nautilus files by SHA-256. The evidence and exports occupy about 2.2 GB; temporary analysis databases need additional working space.

The coordinator now uses up to three copy retries and a 900-second overall limit per role without a shorter API stream deadline. Normal Kubernetes API requests retain their 30-second deadline. Failed staged copies preserve previously collected role data. The fix applies to both single and repeated runs. All 59 local tests passed, including transfer failure and timeout-isolation regressions.

Only this single workload was run. A repeated batch has not started: local disk space and the proposed cloud storage destination must be resolved first. The existing Grafana dashboard displays historical data but still has older formulas and filters; the revised local dashboard has not been imported during this validation.

## Files

- [Exact outcome summary](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/outcome-summary.json)
- [Lag summary](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/lag-summary.json)
- [Execution and resource summary](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/execution-summary.json)
- [Runner status](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/runner-status.json)
- [Evidence integrity report](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/evidence-integrity.json)
- [Frozen configuration](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/pipeline-configmap.yaml)
- [Prometheus CSV](/Users/soheila/Desktop/Thesis-26-27/code/results/run-20260911-214924/prometheus.csv)

The source evidence remains on Nautilus under each role's evidence directory for this run. No old topics, results or source evidence were deleted. The Chrome message blocking PDF/ZIP downloads concerns browser downloads, not these recovered experiment files.
