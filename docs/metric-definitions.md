# Metric definitions and experiment analysis

All JSON latency values are in seconds; multiply by 1,000 for milliseconds. Fractions become percentages by multiplying by 100. These calculations describe the synthetic application currently implemented. No live Nautilus performance result is implied.

## Timing and populations

The frozen manifest defines evaluation start, production end and drain end. The evaluation interval includes its start and excludes its end. Warm-up is excluded. The admitted cohort contains unique broker-acknowledged message IDs whose producer timestamp falls inside that interval. Follow this cohort through the drain bound. A producer timestamp is recorded before enqueue attempts; enqueue waiting therefore contributes to latency.

Completion is after synthetic work and before offset storage/commit. Processing-start latency is start minus producer time. Completion latency is finish minus producer time. Processing duration uses a local monotonic timer around synthetic work, excluding evidence writing and offset bookkeeping. It includes scheduler/sleep delay during that work. APP_DELAY_MS is a requested sleep, not an exact measured duration. Adding work can also increase queueing, so its effect need not be a constant latency shift.

Minimum and maximum completion latency are the smallest and largest valid latencies in the same distinct evaluation-message cohort used for mean and p99, considering first completion by the drain cutoff. They are unavailable when that cohort has no valid completions. Maximum is an observed extreme, not p99 or a bound for unfinished messages; cross-machine clock uncertainty also applies.

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


### Prespecified deadline sensitivity

The [four-hot-partition comparison](../experiments/four-hot-partitions-20260930/README.md) uses a **1-second primary completion deadline** and a **0.5-second sensitivity deadline**, specified before performance trials. Balanced calibration provides context, not an application requirement or a future latency guarantee. These are experimental targets; an acceptable violation percentage has not been specified. Historical 99 ms outcomes retain their original definition.

`SLO_THRESHOLD_MS` records the primary threshold in the run configuration. `completion_deadline_reporting_ms` records both thresholds in the campaign protocol. The evaluator's `completion_deadline_outcomes` uses the same distinct acknowledged evaluation messages, earliest completions and fixed cutoff for each threshold. Late means completion latency strictly exceeds the threshold; overdue unfinished records also count as misses. Censored deadlines or invalid completion clocks make the full-cohort fraction unavailable. The comparison report retains both fractions and their late-completion, overdue-unfinished and censored counts. Observed-completion attempt rates remain separate.


## Grafana and retained evidence

Prometheus supplies counters, histogram buckets, lag, process CPU and memory. Its fixed 30-second rate queries describe rolling attempt populations. Mean latency is sum(rate(latency_sum[30s])) / sum(rate(latency_count[30s])) across consumers. Histogram p99 aggregates bucket rates across consumers before histogram_quantile; it is a bucket-interpolated estimate. Do not average per-consumer or rolling p99 values to obtain a run p99.

Identity matching, exact cohort quantiles, unfinished outcomes and deduplication use unsampled events.jsonl. Aggregate Grafana series cannot reconstruct these identities. Keep final.json and final.prom as process closure evidence. The asynchronous evidence buffer must not drop records in a valid experiment.

Acknowledgment rate is measured at delivery callbacks, so it is a delivery-confirmation rate rather than exact broker arrival timing. Compare achieved rates and partition fractions with the configured target. Process CPU is process CPU time per elapsed time (100% is approximately one CPU core); RSS is resident bytes. These are not Kubernetes allocated resource cost.

## Lag and runtime signals

Since 29 September, the consumer initializes returned-record position from the verified absolute assignment offset while the client still reports `OFFSET_INVALID` and no record has been returned from that partition. The entire returned batch clears this fallback before processing. Fresh broker bounds must still validate the offset. `consumer_client_position_offset` retains the raw value, and `consumer_position_from_assignment` identifies this initialization. This prevents unresolved client initialization from hiding known backlog after handoff; it does not fill missing historical samples or bypass ownership and freshness checks. See the [monitoring handover check](../experiments/monitoring-gap-20260929/README.md).

AWS documents a related monitoring limitation in Amazon MSK: consumer-lag metrics are emitted only for groups in the `STABLE` or `EMPTY` states and are absent while a group is unstable. The stable state follows successful rebalancing. This is a limitation of metric availability, not evidence of lost messages. It describes MSK behavior; our longer post-assignment gaps were separately traced to unresolved client positions, as documented above. See [AWS: Monitor consumer lags](https://docs.aws.amazon.com/msk/latest/developerguide/consumer-lag.html) (accessed 29 September 2026).

analyze_lag.py reads the exported run-scoped Prometheus series. A partition snapshot needs exactly one current owner, fresh valid lag and finite offsets. Every expected partition must be represented. Missing/stale data is not zero. Lag is high offset minus returned-record position. Processing backlog uses the completion frontier and is a separate metric. These are offset spans, not exact record counts when offsets have gaps.

| Analyzer convention | Calculation and interpretation |
|---|---|
| Partition and total lag | Per-partition lag L = H − R for valid H ≥ R; total B = sum of L over all partitions. The max(0, H−R) notation must never turn an invalid negative observation into valid zero. |
| Attempt throughput (diagnostic) | Completed attempt count / common evaluation duration. |
| Completion latency | Completion time minus producer time. |
| Attempt SLO rate (diagnostic) | 100 × observed late valid completion attempts / observed valid completion attempts; distinct from the primary admitted-cohort deadline outcome. |
| Instantaneous backlog growth | (Current B − previous B) / actual elapsed seconds. |
| Window backlog growth | (Latest B − B from m intervals earlier) / elapsed time: m+1 consecutive valid snapshots. |
| Partition mean, maximum and skew | Mean L = B / partition count; max L; skew = max L / mean L, or zero when all lag is zero. |
| Mean skew | Arithmetic mean of skew over the last m valid contiguous snapshots. |
| Hot partitions | Hot when L > minimum_lag AND L > mean L + k × population standard deviation. Both inequalities are strict. |
| Hot-partition persistence | Persistence = count of hot observations / m. Persistently hot requires hot now AND persistence ≥ threshold. |

Attempt-throughput and observed-attempt deadline rates are diagnostics; the proposal uses distinct completion throughput and acknowledged-cohort SLO outcomes.

Population standard deviation divides squared deviations by the partition count. Window means and persistence use m snapshots. Growth uses m intervals. Windows reset after missing observations, excessive gaps, detected ownership changes or decreasing offsets. Unsampled changes cannot be reconstructed. At a 2-second export step, 15 growth intervals span 30 seconds but 15 sample points span 28 seconds; configure and report these explicitly.

The full-evaluation mean total backlog and mean skew use ordinary arithmetic averages: sum the valid individual measurements of each metric and divide by their count. Each valid sample has equal weight; missing measurements are excluded rather than counted as zero. These summaries use the same arithmetic averaging rule as the rolling means but cover all valid evaluation samples instead of the latest 15. They do not average overlapping rolling means. The report stores these values as `sample_mean`; `time_weighted_mean` remains a separate diagnostic and is not used for the displayed backlog/skew means. Lag coverage still measures valid continuous time, excluding invalid intervals, ownership changes and long gaps. Peak lag is the largest valid sampled total. Final lag requires a valid sample exactly at the evaluation boundary; otherwise it is null.

Mean backlog before mitigation in the twelve-partition results is the arithmetic mean of 15 total-backlog measurements at evaluation seconds 30, 32, ..., 58. This is the final complete rolling window before the scheduled action at 60 seconds; the Keep 3 reference uses the same window. It spans 28 seconds between its first and last samples, approximately 0:30–1:00 after warm-up. All 15 observations must be valid and contiguous with unchanged ownership and nondecreasing offsets. Missing observations leave the result unavailable rather than selecting an earlier window. A 1 ms tolerance accommodates export timestamp rounding at the nominal two-second grid, without interpolating values. This recent-window mean covers 15 samples; the mean total backlog across the full evaluation covers all valid samples.

Mean backlog at evaluation end uses the same calculation on the last 15 measurements before production stops: seconds 570, 572, ..., 598 in a ten-minute evaluation. The same validity and continuity checks apply, and Keep 3 uses the same window. It describes recent backlog while input continues, before drain. The full-evaluation mean still includes the earlier accumulation and recovery period; it can remain large even when this ending mean is low. Neither mean counts distinct unfinished messages.

Table 5 labels the ending mean **Eval. end mean backlog**. **Eval. end total backlog** is the total at the last sample in that window, at evaluation second 598. Both describe the end of evaluation while input continues; Keep 3 is sampled at the same times. The minimum-total-backlog column remains in LaTeX comments and in the stored analysis, but is not displayed.

The twelve-partition outcome figure shows total backlog and acknowledged unfinished messages on axes that are linear up to one and logarithmic above one, preserving zero values. The total-backlog axis uses the same scale transformation as the rolling-backlog figure, making a small positive backlog visible alongside large peaks. Completion p99 and unfinished percentages in its lower panels retain linear axes.

Min total backlog is the smallest total consumer-position lag among valid evaluation samples. Min partition lag is the smallest lag across every partition in those valid samples, not the minimum of the maximum-partition-lag series. Missing observations do not contribute zeros, and minima are unavailable when no valid observations exist. A zero partition minimum means that at least one partition was caught up at one observed moment; other partitions may still have backlog. These are sampled diagnostic extrema, not guarantees about the unobserved intervals or controller inputs.

Defaults are exploratory: window_samples=15, hot_k=1, minimum_lag=10, persistence=0.8, max_gap=3 seconds. They are analyzer options, not automatically calibrated thresholds or controller settings. With eight zero-lag partitions and two equal hot partitions, k=2 puts the threshold exactly on the hot value; strict > detects none. Calibrate on pilot runs before comparisons.

The growth window can be set independently with `growth_window_samples` (CLI:
`--growth-window-samples`). Five intervals on a 2-second export grid give a
10-second growth window using six observations. The numerator is the change in
lag or processing backlog; the denominator is the actual elapsed time between
the endpoints. This setting leaves the window mean and persistence settings
unchanged. When omitted, the analyzer retains the historical `window_samples`
intervals for growth.

The four-condition comparison dated 29 September displays 10-second growth
for all eight trials, recomputed from saved snapshots. The five-minute-evaluation
ownership, scale-and-redistribute and redistribute-within-three comparison
figures dated 27–28 September use the same 10-second window. Older archived
figures retain their recorded window settings. Missing observations, ownership changes, gaps
longer than three seconds and offset resets still restart the growth window.
The shorter window is a descriptive analysis setting, not a change to the
predefined 30-second low-backlog recovery criterion or rolling Grafana queries.

The consumer serializes lag/ownership updates with metrics exposition so a scrape cannot mix a new position with an older high offset. The exported completion offset is refreshed with the lag snapshot. This consistency is local to one consumer; observations across consumers still have different timestamps and must pass freshness/ownership checks. A scrape may wait for a bounded lag query or ownership callback; missing scrapes remain missing. Calibration run `run-20260912-053411` exposed the earlier mixed-field issue and retains its original 85% coverage result. The correction does not retroactively repair those observations.

## Several runs

summarize_runs.py separates matching frozen configurations, actual initial replica counts, evaluation/follow-up durations and recorded source/package signatures. Different workload seeds remain separate groups; paired seed comparisons require a further analysis. Invalid runs remain listed with reasons and do not enter pooled results. A valid run with poor performance stays included.

For a run-level metric x over R matching runs: mean = sum(x)/R; sample standard deviation = sqrt(sum((x−mean)^2)/(R−1)). Standard deviation is null for one run. These are repetition summaries, not confidence intervals. The mean of run p99 values describes the typical run's p99; it is not a pooled p99.

Pooled mean = sum of every valid message latency / total valid message count. Pooled p99 sorts those message latencies together. Pooled unfinished/deadline fractions use summed counts and denominators. Runs with more messages have more weight in pooled statistics. Do not pool different workloads/policies to claim one treatment result.

Worked arithmetic: the 20-message example totals 1,240 ms, mean 62 ms. If the 150 ms message is unfinished, 19 observed completions total 1,090 ms, mean 57.368 ms and nearest-rank p99 100 ms. With a 99 ms deadline, two 100 ms completions plus the overdue unfinished message give 3/20 = 15% deadline misses; unfinished is 1/20 = 5%.

## Reporting before and after intervention

For ten-minute mitigation trials, the before-action interval is evaluation seconds [30,60), the post-activation interval runs from confirmed assignment to second 600, and the final interval is [570,600). Keep 3 uses second 60 as its reference. These intervals exclude drain; activation does not imply recovery. Backlog and skew means use valid original two-second measurements with equal weight, not overlapping rolling means. The before and final intervals each expect 15 samples, ending at seconds 58 and 598. Report valid/expected counts.

Completion throughput counts distinct first completions in each interval, including warm-up messages completed then, divided by interval seconds. Interval p99 instead uses acknowledged evaluation messages whose first completion occurs in that interval. It retains each original producer timestamp, including waiting before activation, and excludes warm-up messages. Whole-run cohort outcomes still follow evaluation-produced messages through drain end. These populations must not be mixed.

## Measured arrival rate and consumer capacity

Partition arrival rate is estimated from the increase in producer acknowledgment counters divided by actual elapsed seconds, summed across producers. It is a proxy for broker arrival, not the configured target or an exact broker-arrival timestamp. The capacity analysis uses the thirty-second pre-action interval ending at nominal second 58, with fresh endpoints, unchanged process identity and no counter reset. Missing interior samples can be bridged only when cumulative endpoints remain valid.

Assigned input is the sum of measured partition rates for each consumer's assignment. Resulting-assignment comparisons reuse those pre-action partition rates and assume unchanged input; they are not post-action arrival measurements. Native Kafka's resulting assignments are assessed retrospectively.

Effective capacity is completed attempts divided by elapsed wall time in eligible calibration intervals with sustained available work, including polling, commits and other processing-loop overhead. The analysis uses nonoverlapping sixty-second intervals after evaluation second 60, stable ownership and process identity, fresh counters, valid contiguous processing backlog, ten-second throughput coefficient of variation at most 0.10, and minimum assigned processing backlog at least max(100, twice the observed messages/s). The preceding ten seconds are also checked. Pool eligible counts over eligible elapsed seconds; do not interpret interval subdivisions as independent trials.

Calibration references match recorded nodes, resource requests and processing settings. CPU requests alone cannot establish messages/s capacity. The original C2 estimate is 291.5 messages/s from 112 eligible intervals in six runs; revised C1 is 213.9 messages/s from one interval. Other rates are demonstrated unsaturated throughput, and added-consumer capacities remain unmeasured. Capacity minus assigned input is a conditional margin for that consumer; positive margins for every consumer do not guarantee a recovery time, p99 target or worthwhile intervention cost. [Evidence and limitations](../experiment-records/signal-outcome-20261021/CAPACITY_CHECK.md) and [assignment checks](../experiment-records/signal-outcome-20261021/assignment-capacity.csv) retain the full calculation.

The demand-versus-capacity framing follows the Kafka provisioning studies cited in the proposal: Diogo Landau et al., *Latency and Cost-Aware Consumer Group Autoscaling in Message Broker Systems*, Journal of Parallel and Distributed Computing 201 (2025), 105071; and Mazen Ezzeddine et al., *Latency Aware and Resource-Efficient Bin Pack Autoscaling for Distributed Event Queues: Parameters Impact and Setting*, SN Computer Science 6 (2025), 219. The acknowledgment-counter estimator and eligibility rules above describe this implementation, not a claim that those papers used identical measurements.

## Intervention timing

Decision to active ends when the intended assignment is verified. Normal Kafka scaling requires an unchanged six-consumer assignment across ten valid seconds; targeted actions use the explicit handover confirmation. Global gap is the longest interval during the transition when no consumer is processing, not a gap between two particular consumers.

Pending change before resume is the acknowledged-but-unfinished message count at the first processing start after the resume request minus the count at partition-release request. This interval differs from assignment verification, which can continue after processing resumes. Negative values mean unfinished work decreased. Normal Kafka scaling has no explicit release/resume procedure, so this value is unavailable; no-action runs also have no value. Net pending during transfer is not a reported proposal metric.

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

The coordinator supports scheduled no-action, native scaling, targeted partition redistribution and scaling with targeted redistribution. Its intervention time is relative to evaluation start, excluding warm-up. The manifest records the declared plan and initial partition-to-pod assignment. Action journals separate scheduled time, actual decision and completion of the scale API request. Resource observations retain the Kubernetes Ready transition timestamp plus when the coordinator first observed readiness. Consumer events record assignment/revocation callback entry and exit; completion records include the ownership epoch. `execution-summary.json` reports callback blocking intervals and assignment-to-first-completion delays. The latter includes idle time and application work, so it is not an isolated handover pause or broker rebalance duration.

Optional recovery uses a declared total processing-backlog threshold and hold duration. It requires valid contiguous same-owner snapshots at or below the threshold throughout the sampled hold interval; a gap, invalid sample or owner change restarts the interval. Recovery time is confirmation time (end of the hold) minus the common intervention anchor. For anchor 0, backlog at or below 5 offsets at times 2, 4, 6 and 8 s and hold 6 s, recovery is confirmed at 8 s. If it is not observed by evaluation end, time is null and the result is censored. Missing coverage is explicitly unavailable. This is backlog recovery only; joint latency/growth recovery is a separate planned definition.

No automatic threshold tuning or policy selection is implemented. The current reported experimental deadline is one second, with a half-second sensitivity analysis. These thresholds were specified before the four-partition trials and applied retrospectively to the twelve-partition trials. The original 99 ms measurements retain their original settings. Scheduled targeted redistribution is implemented through coordinated exclusive-ownership handover, as documented in the experiment protocols; it is not a broker replica move.

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

## Retained process-resource measurements and processing-backlog growth

New complete runs save `measurement-audit.json` automatically. It reports missing
CPU/RSS series or their original scrape timestamps for every collected process
incarnation and checks that outcome, lag and execution summaries exist. Missing
required process measurements fail the run's measurement checks while preserving
all collected evidence and outcome analysis. This is not proof of complete coverage
of every proposal metric, container, broker or node.

The Prometheus export retains CPU/RSS scrape timestamps in addition to the raw
process gauges. Per-process evaluation and evaluation-plus-drain summaries report
covered duration, mean, peak and integral. Mean is trapezoidal area divided by
covered seconds. CPU percent divided by 100 gives cores; its integrated value
divided by 100 gives observed CPU-seconds. Resource tables and integral plots divide
this unrounded CPU total by 60 to report core-minutes, matching requested CPU
units; the saved `actual_cpu_core_seconds` field retains its original unit. Memory remains RSS bytes. Only finite,
nonnegative values with scrape age at most ten seconds contribute; adjacent samples
must be at most three seconds apart. No extrapolation or missing-to-zero conversion
is performed. Scrape freshness does not prove the exporter sampling loop is fresh.
Coverage uses the whole window and therefore includes time before a scaled process
starts; it must not be interpreted as live-process coverage. These are not
whole-pipeline aggregate peaks, pod working sets, or requested-resource cost.

`lag-summary.json` now also contains instantaneous and windowed processing-backlog
(Q) growth in offsets/second. Q growth is (Q_now-Q_previous)/elapsed time, while
window growth uses m intervals and m+1 contiguous snapshots. These are distinct
from existing consumer-position lag (B) growth. Missing snapshots, ownership
changes and offset resets interrupt both growth calculations. Longer windows can
be recomputed from retained valid snapshots, subject to coverage; absent monitoring
cannot be reconstructed. Growth alone does not establish infinite-horizon stability.

Retain `events.jsonl`, `final.json`, `final.prom`, the frozen manifest and run config,
raw `prometheus.json`/CSV with timestamp series, resource histories, intervention
journals and analysis summaries. Small Git records do not replace a separate backup
of raw evidence. Container/broker resource monitoring and complete sampling-loop
freshness remain unverified; historical exports lacking timestamps remain limited.
