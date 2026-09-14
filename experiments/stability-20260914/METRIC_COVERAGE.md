# Metric coverage audit — 14 September 2026

Checked against current runtime/analyzers, docs/metric-definitions.md and the local
proposal worked-example source. A current online proposal comparison is pending;
this is not a claim that every metric in the live proposal is implemented.

| Metric family | Existing calculation/evidence | Status for new trials |
|---|---|---|
| Completion mean, p50/p95/p99 | Earliest valid distinct admitted-cohort completions through drain; exact nearest-rank quantiles | Implemented; requires complete event evidence |
| Processing-start latency and work duration | Earliest cohort completion diagnostics; duration uses monotonic clock | Implemented |
| Unfinished/deadline outcomes | Admitted IDs reconciled with completions; censoring and invalid clocks explicit | Implemented; 99 ms deadline remains provisional |
| Admission, achieved acknowledgments, useful and attempt throughput | Producer/consumer events, with distinct populations and common evaluation boundaries | Implemented; rolling plots must not replace exact run totals |
| Per-partition outcomes | Admission/completion counts, throughput and cohort outcomes | Implemented |
| Partition/total lag and processing backlog | Valid, fresh ownership-complete offset snapshots | Implemented; offset spans are not always record counts |
| Lag growth, mean/max/skew, window features, hotness/persistence | analyze_lag.py; explicit thresholds and gap handling | Implemented offline; defaults are exploratory |
| Backlog area/peak and recovery | Covered intervals and declared backlog threshold/hold duration | Implemented; recovery is not sustained stability |
| Duplicate attempts and consistency checks | Identity reconciliation, output hashes, ordering diagnostics | Implemented; does not prove durable application effects |
| Scaling/rebalance timing | Action, readiness and callback/first-completion records | Implemented; not an isolated broker rebalance duration |
| Consumer requested CPU/GiB-seconds | Sampled Kubernetes resource requests integrated over time | Implemented; requested cost is not actual usage |
| Producer and consumer process CPU | psutil process CPU percent gauges; 100 percent means approximately one core | Exported in run-scoped Prometheus JSON/CSV; live completeness pending |
| Producer and consumer process memory | psutil resident-set bytes gauges | Exported in run-scoped Prometheus JSON/CSV; live completeness pending |
| Container/pod CPU, working-set memory, throttling and broker usage | Requires additional verified monitoring series | Not guaranteed by current consumer/producer-only export |
| Whole-cluster resource cost and full mitigation-policy efficacy | Broader infrastructure accounting and validated policies | Not established by these trials |

For CPU plots, convert percent to cores by dividing by 100. Sum simultaneous
per-process cores only when all expected live processes are covered. For memory,
sum simultaneous process RSS and convert bytes to MiB/GiB; this is process memory,
not pod working set or reserved memory. Report per-process and aggregate traces,
covered duration, mean and peak; do not fill missing samples with zero. Existing
run summaries do not yet automatically produce these CPU/RSS aggregates.

Report each run separately, then descriptive mean/range of like-for-like runs.
Averaging two run p99s is not a pooled-message p99. Two repetitions do not justify
strong uncertainty or infinite-horizon stability claims. The new sustained-window
trend report and CPU/RSS aggregate report still need implementation/validation
before these trials can be described as a complete stability evaluation.
