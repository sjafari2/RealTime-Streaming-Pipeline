# Metric definitions and experiment analysis

All JSON latency values are in seconds; multiply by 1,000 for milliseconds. Fractions become percentages by multiplying by 100. These calculations describe the synthetic application currently implemented. No live Nautilus performance result is implied.

## Timing and populations

The frozen manifest defines evaluation start, production end and drain end. The evaluation interval includes its start and excludes its end. Warm-up is excluded. The admitted cohort contains unique broker-acknowledged message IDs whose producer timestamp falls inside that interval. Follow this cohort through the drain bound. A producer timestamp is recorded before enqueue attempts; enqueue waiting therefore contributes to latency.

Completion is after synthetic work and before offset storage/commit. Processing-start latency is start minus producer time. Completion latency is finish minus producer time. Processing duration uses a local monotonic timer around synthetic work, excluding evidence writing and offset bookkeeping. It includes scheduler/sleep delay during that work. APP_DELAY_MS is a requested sleep, not an exact measured duration. Adding work can also increase queueing, so its effect need not be a constant latency shift.

| Metric | Exact calculation | Output in outcome-summary.json |
|---|---|---|
| Completion mean | Sum of valid earliest cohort completion latencies / their count | admitted_cohort_completion_mean_seconds |
| Completion p50 / p95 / p99 | Sort valid cohort latencies; select 1-based rank ceil(q × count), q = 0.50 / 0.95 / 0.99 | admitted_cohort_completion_p50_seconds / p95_seconds / p99_seconds |
| Unfinished fraction | Cohort IDs without completion by drain / admitted cohort size | incomplete_fraction |
| Primary deadline misses | Late completed cohort IDs plus unfinished IDs whose deadline passed | deadline_misses |
| Primary deadline miss fraction | Primary misses / admitted cohort size; undefined if deadlines are censored or clocks invalid | deadline_miss_fraction |
| Admission rate | Admitted cohort size / evaluation seconds | admitted_messages_per_second |
| Completed attempt throughput | All completion events inside evaluation / evaluation seconds; includes replays | completed_attempts_per_second |
| Useful throughput | Distinct IDs completing inside evaluation / evaluation seconds | useful_throughput_per_second |
| Observed completion violation fraction | Late valid completion attempts inside evaluation / valid completion attempts inside evaluation | observed_completion_deadline_miss_fraction |
| Processing-start and processing duration | Mean and nearest-rank p50/p95/p99 for available valid diagnostics on earliest cohort completions | diagnostic_cohort_latencies |

Useful throughput uses completion-time membership; cohort latency uses producer-time membership. They need not have the same numerator. A warm-up message completing in evaluation contributes to useful throughput but not cohort latency. A cohort message completing in drain contributes to cohort latency but not evaluation throughput.

An unfinished message has no observed completion latency. It contributes neither zero nor a fabricated deadline latency to mean/p99. Report unfinished and deadline outcomes alongside conditional latency statistics. If drain ends before a pending message's deadline, its deadline outcome is censored. Empty latency populations have null mean/p99, not zero. Missing diagnostic fields in older evidence reduce the corresponding diagnostic count.

The evaluator detects missing final snapshots, outcome loss/errors, unresolved sends, invalid cohort clocks and inconsistent replay outputs. Review validity_failures before using a result. Absence of a detected failure does not establish synchronized clocks, correct infrastructure or scientific validity. Failed/cancelled sends are counted separately for the whole run. Duplicate attempts count extra completions for admitted IDs through drain; hashes check repeat output consistency, not a durable sink.

## Grafana and retained evidence

Prometheus supplies counters, histogram buckets, lag, process CPU and memory. Its fixed 30-second rate queries describe rolling attempt populations. Mean latency is sum(rate(latency_sum[30s])) / sum(rate(latency_count[30s])) across consumers. Histogram p99 aggregates bucket rates across consumers before histogram_quantile; it is a bucket-interpolated estimate. Do not average per-consumer or rolling p99 values to obtain a run p99.

Identity matching, exact cohort quantiles, unfinished outcomes and deduplication use unsampled events.jsonl. Aggregate Grafana series cannot reconstruct these identities. Keep final.json and final.prom as process closure evidence. The asynchronous evidence buffer must not drop records in a valid experiment.

Acknowledgment rate is measured at delivery callbacks, so it is a delivery-confirmation rate rather than exact broker arrival timing. Compare achieved rates and partition fractions with the configured target. Process CPU is process CPU time per elapsed time (100% is approximately one CPU core); RSS is resident bytes. These are not Kubernetes allocated resource cost.

## Lag and proposal equations 1–17

analyze_lag.py reads the exported run-scoped Prometheus series. A partition snapshot needs exactly one current owner, fresh valid lag and finite offsets. Every expected partition must be represented. Missing/stale data is not zero. Lag is high offset minus returned-record position. Processing backlog uses the completion frontier and is a separate metric. These are offset spans, not exact record counts when offsets have gaps.

| Equations | Calculation and interpretation |
|---|---|
| 1–2 | Per-partition lag L = H − R for valid H ≥ R; total B = sum of L over all partitions. The max(0, H−R) notation must never turn an invalid negative observation into valid zero. |
| 3 | Completed attempt count / common evaluation duration. |
| 4 | Completion time minus producer time. |
| 5 | 100 × observed late valid completion attempts / observed valid completion attempts; distinct from the primary admitted-cohort deadline outcome. |
| 6 | (Current B − previous B) / actual elapsed seconds. |
| 7 | (Latest B − B from m intervals earlier) / elapsed time: m+1 consecutive valid snapshots. |
| 8–10 | Mean L = B / partition count; max L; skew = max L / mean L, or zero when all lag is zero. |
| 11 | Arithmetic mean of skew over the last m valid contiguous snapshots. |
| 12–13 | Hot when L > minimum_lag AND L > mean L + k × population standard deviation. Both inequalities are strict. |
| 14–16 | Persistence = count of hot observations / m. Persistently hot requires hot now AND persistence ≥ threshold. |
| 17 | Ordered tuple of window mean B, window growth, window mean skew and persistent hot set. Other controller inputs are still needed. |

Population standard deviation divides squared deviations by the partition count. Window means and persistence use m snapshots. Growth uses m intervals. Windows reset after missing observations, excessive gaps, detected ownership changes or decreasing offsets. Unsampled changes cannot be reconstructed. At a 2-second export step, 15 growth intervals span 30 seconds but 15 sample points span 28 seconds; configure and report these explicitly.

Time-weighted run mean lag uses trapezoidal area / covered seconds. Each eligible interval contributes (B_previous + B_current)/2 × elapsed seconds. Coverage excludes invalid intervals and long gaps. Report coverage with area and mean; never silently bridge missing periods. Peak lag is the largest valid sampled total. Final lag requires a valid sample exactly at the evaluation boundary; otherwise it is null.

Defaults are exploratory: window_samples=15, hot_k=1, minimum_lag=10, persistence=0.8, max_gap=3 seconds. They are analyzer options, not automatically calibrated thresholds or controller settings. With eight zero-lag partitions and two equal hot partitions, k=2 puts the threshold exactly on the hot value; strict > detects none. Calibrate on pilot runs before comparisons.

The consumer serializes lag/ownership updates with metrics exposition so a scrape cannot mix a new position with an older high offset. The exported completion offset is refreshed with the lag snapshot. This consistency is local to one consumer; observations across consumers still have different timestamps and must pass freshness/ownership checks. A scrape may wait for a bounded lag query or ownership callback; missing scrapes remain missing. Calibration run `run-20260912-053411` exposed the earlier mixed-field issue and retains its original 85% coverage result. The correction does not retroactively repair those observations.

## Several runs

summarize_runs.py separates matching frozen configurations, actual initial replica counts, evaluation/follow-up durations and recorded source/package signatures. Different workload seeds remain separate groups; paired seed comparisons require a further analysis. Invalid runs remain listed with reasons and do not enter pooled results. A valid run with poor performance stays included.

For a run-level metric x over R matching runs: mean = sum(x)/R; sample standard deviation = sqrt(sum((x−mean)^2)/(R−1)). Standard deviation is null for one run. These are repetition summaries, not confidence intervals. The mean of run p99 values describes the typical run's p99; it is not a pooled p99.

Pooled mean = sum of every valid message latency / total valid message count. Pooled p99 sorts those message latencies together. Pooled unfinished/deadline fractions use summed counts and denominators. Runs with more messages have more weight in pooled statistics. Do not pool different workloads/policies to claim one treatment result.

Worked arithmetic: the 20-message example totals 1,240 ms, mean 62 ms. If the 150 ms message is unfinished, 19 observed completions total 1,090 ms, mean 57.368 ms and nearest-rank p99 100 ms. With a 99 ms deadline, two 100 ms completions plus the overdue unfinished message give 3/20 = 15% deadline misses; unfinished is 1/20 = 5%.

## Capacity, cost and recovery

The coordinator estimates balanced offered rate per consumer = TARGET_RATE × actual producers / actual consumers. Its reciprocal × 1,000 is a theoretical milliseconds-per-message work budget. For 3 producers × 3,000/s / 6 consumers: 1,500/s each and 0.667 ms/message. This assumes balance and ignores overhead; it is not a measured service time or latency prediction. APP_DELAY_MS=0 is a useful baseline, then calibrate work in pilots.

Allocated cost requires resource histories: consumer-seconds = integral of consumer count; allocated CPU-seconds = integral of CPU requests; allocated GiB-seconds = integral of requested memory. Process CPU/RSS gauges do not substitute for these. The implemented process lifetime and sampled consumer-container request integrals are described below. Whole-cluster accounting, paired confidence intervals and the adaptive selector remain planned.

Sources: [Confluent Python API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html) for offset semantics; [Prometheus histograms](https://prometheus.io/docs/practices/histograms/) for aggregating distributions and quantile approximation.


## Partition results and correctness checks

`outcome-summary.json` now includes `partition_metrics.partitions`, with one row per expected topic/partition. The common evaluation duration is the denominator of each rate. Admission counts distinct acknowledged IDs produced inside evaluation; acknowledged arrival counts distinct IDs whose delivery callbacks occur inside evaluation. It is not an exact broker-arrival timestamp. Unique processing throughput counts distinct IDs completing inside evaluation; attempt throughput includes repeated completions. A partition with no admissions has undefined cohort latency and fractions. Its measured count/rate can still be zero. Older evidence missing partition metadata has unavailable partition results, not rows of invented zeros.

Each partition's cohort mean, p50/p95/p99, unfinished fraction and deadline fraction use the same identity and earliest-completion rules as the pipeline. For the worked 20-message list, p50 is 60 ms, p95 is 100 ms and p99 is 150 ms. Removing the illustrative unfinished 150 ms value leaves p50 = 60 ms and p95 = p99 = 100 ms.

The evaluator checks that one logical message ID agrees on producer timestamp and Kafka topic/partition/offset, and that one physical Kafka record does not claim different IDs. It checks decreasing offsets within one process and assignment epoch. Equal offsets are repeated attempts; a new ownership epoch starts a new ordering sequence. These checks do not prove order at a downstream sink. There is no durable business-effect sink in the synthetic application, so `application_effects_checked` is false. Unfinished by drain is not evidence of permanent message loss.

## Execution and resource accounting

`execution-summary.json` contains every persisted consumer process lifetime, including removed pods and restarted incarnations. Consumer process-seconds = sum of elapsed start-to-finish lifetimes. New evidence uses each process's local monotonic timer; legacy evidence uses a labeled wall-clock fallback. This includes preparation, warm-up and drain; the final scrape hold after the finished event is outside that process-lifetime definition. If closure timestamps are missing, the total is unavailable. For two lifetimes of 15 s and 17 s, consumer process-seconds are 32, regardless of how many pods survive collection.

The coordinator also samples the Kubernetes API about every 5 seconds into `resource-history.jsonl`, starting during preparation and ending after cleanup. For adjacent valid samples no more than 15 seconds apart, it holds the left sample's scheduled pod counts and consumer-container requests until the next sample. It integrates pod count × seconds, requested CPU cores × seconds, and requested GiB × seconds. It includes scheduled but not-ready and terminating pods, so startup and shutdown are represented. Unscheduled pods are excluded. Pod membership changes remain in the history after pods disappear. Transition-time uncertainty can be as large as a sample gap.

These are observed request-time estimates for the consumer application container, not actual CPU/RSS usage or the full deployment cost. Sidecars, brokers, producers and controllers are outside this accounting. Pod-level requests and a detected active resize are flagged as unsupported. Gaps are excluded and coverage is reported; zero covered seconds gives unavailable cost, not zero. To compare trials, report cost and coverage together over equivalent observation windows. Kubernetes requests are resource declarations: [Kubernetes resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).

## Scheduled pilots and recovery

The optional pilot runner supports scheduled no-action and adding consumers. Its intervention time is relative to evaluation start, excluding warm-up. The manifest records the declared plan and initial partition-to-pod assignment. Action journals separate scheduled time, actual decision and completion of the scale API request. Resource observations retain the Kubernetes Ready transition timestamp plus when the coordinator first observed readiness. Consumer events record assignment/revocation callback entry and exit; completion records include the ownership epoch. `execution-summary.json` reports callback blocking intervals and assignment-to-first-completion delays. The latter includes idle time and application work, so it is not an isolated handover pause or broker rebalance duration.

Optional recovery uses a declared total processing-backlog threshold and hold duration. It requires valid contiguous same-owner snapshots at or below the threshold throughout the sampled hold interval; a gap, invalid sample or owner change restarts the interval. Recovery time is confirmation time (end of the hold) minus the common intervention anchor. For anchor 0, backlog at or below 5 offsets at times 2, 4, 6 and 8 s and hold 6 s, recovery is confirmed at 8 s. If it is not observed by evaluation end, time is null and the result is censored. Missing coverage is explicitly unavailable. This is backlog recovery only; joint latency/growth recovery is a separate planned definition.

No automatic threshold tuning or policy selection is implemented. The 99 ms deadline, workload durations and pilot command values are provisional and require calibration. Exact targeted whole-partition reassignment still needs a supported assignment design; the runner does not substitute a broker replica move or arbitrary local assignment override.

## Combining the new metrics across repetitions

The batch summary groups the declared intervention and recorded initial assignment as well as configuration, replica counts, durations and source/package signatures. Generated topic prefixes are normalized, so partition 0 of topic 0 can be compared across fresh-topic runs. Different initial ownership maps remain separate groups; the broker assignor is not forced to recreate an identical map.

Pipeline and partition p50/p95/p99 are reported both as run-level metrics and as pooled message quantiles. Pooled quantiles sort the individual valid latencies; they never average run quantiles. Partition rates and request-time costs have run-level mean, sample standard deviation, minimum, maximum and available-run count. Recovery summaries list the number of censored trials; their recovery-time mean includes observed recoveries only and must not be presented as the mean of all trials. Runs marked interrupted/failed are explicitly excluded even if their individual messages look complete. Poor performance alone does not exclude a valid run.

## Explicit paired comparison files

`python-scripts/compare_runs.py` reads an index with a `pairs` list. Each entry supplies `pair`, `none` and `scale`, where the latter two are completed run IDs. Run `python3 python-scripts/compare_runs.py comparison-index.json --output results/comparison` after collecting both treatments. The index contains observed run IDs, not planned run names.

Use a separate index for each workload/duration/application-version family. The analyzer verifies frozen workload settings, application hashes, Python/package signatures and the shared intervention timing. It preserves every individual run and quality flag. Summary paired differences are scale minus no action. Compatible completed runs with valid identity evidence contribute message outcomes even when monitoring is incomplete; this prevents transition-related monitoring loss from hiding an unfavorable outcome. Coverage screens apply only to backlog and resource aggregates, with a separate pair count for each metric. Low achieved admission remains visible and limits a matched-load causal interpretation. A mean of paired differences is a descriptive run-level result; neither an averaged p99 nor its paired difference is a pooled-message p99. There is no small-sample confidence interval claim.

Processing-backlog area uses the same valid intervals as lag, integrating the completion backlog with trapezoids and reporting its covered duration. The primary cost window spans evaluation start to drain end in both treatments, with separate production, evaluation and post-action windows. Requested CPU-seconds are summed only across observed valid intervals; below 95% coverage, the analyzer flags the cost comparison. An unfinished fraction is always retained beside conditional completion latency. Commit-result records and request-to-readiness/first-completion timings remain available for explaining transition behavior.

## Observation freshness revision (12 September 2026)

New managed runs record `lag_freshness_clock=monotonic_scrape_v2`. For partition p
at query time t, the accepted age is `a_local + (t - t_scrape)`. Here `a_local` is
the consumer's monotonic elapsed time since the valid offset query, refreshed
under the same lock as lag/ownership when exporting metrics. `t_scrape` is the
original Prometheus sample timestamp, exported with `timestamp()` as
`consumer_lag_scrape_timestamp_seconds`. The range-query evaluation timestamp
alone does not reveal the original sample time.

Both age components must be finite and nonnegative, and their sum must be at
most `LAG_FRESHNESS_SECONDS` (10 seconds in this campaign). Missing metrics,
invalid offsets and incomplete/duplicate ownership still invalidate a snapshot.
A main-loop stall ages the local observation; an absent scrape ages the retained
sample. Neither is treated as fresh just because Prometheus can return a value.
The live campaign guard and saved analysis use the same predicate.

This is a conservative freshness estimate at scrape resolution: scrape request,
exporter lock waiting and rendering take time. It is not exact simultaneous
observation across consumers. The wall-clock observation timestamp remains a
diagnostic, and message latency still requires sufficiently synchronized producer
and consumer clocks. This change does not establish clock synchronization for
message latency. Older exports retain their original wall-clock rule and explicit
coverage gaps; no previous result is retrospectively repaired.

The first long scale trial (`run-20260912-060649`) was interrupted by the old rule:
two otherwise coherent partition samples had observation timestamps about 28 and
34 milliseconds ahead of the query time. The cause is consistent with scrape
stamping/render timing or a small node-clock offset; the evidence does not isolate
the two. This diagnostic remains excluded from planned full-duration comparisons.

Reference: [Prometheus timestamp() documentation](https://prometheus.io/docs/prometheus/latest/querying/functions/#timestamp).

Protocol revision 3 corrects the PromQL union used to retain sample timestamps.
The union explicitly matches on metric name; otherwise the timestamp series is
suppressed by the raw age series with the same non-name labels. Managed complete
runs verify the actual Prometheus response before production is released. On a
fresh empty topic, an unresolved Kafka position is allowed only as an explicit
invalid/NaN observation with the complete age schema and a fresh original scrape
timestamp. The manifest separately counts valid and unresolved positions. No lag
value is invented. Once production begins, the full observation-freshness rule
applies; the calibration warm-up precedes the live guard evaluation window. Manual `start` without `PROM_URL`
records that this external check was not performed. The short, pre-action attempt
`run-20260912-062803` is retained as a query diagnostic, not a scaling result.

Protocol revision 4 records this empty-topic initialization rule. The preceding
readiness attempt (`run-20260912-063614`) sent no workload: it was correctly
blocked by a check that was too strict for positions on a fresh empty topic.
The production validity rule, limits, workload and scheduled action are unchanged.
