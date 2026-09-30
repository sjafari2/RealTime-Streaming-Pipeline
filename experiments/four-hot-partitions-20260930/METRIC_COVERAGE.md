# Metric coverage for the four-hot-partition comparison

These diagnostics are computed from retained message events and Prometheus exports. Missing measurements remain unavailable. This table distinguishes implemented measurements from planned mitigation policies; recording a feature does not mean an adaptive controller uses it.

| Metric | Calculation | Evidence and reporting |
|---|---|---|
| Per-partition lag | Broker high offset minus returned-record position, under valid unique ownership | `lag-summary.json`: `lags` |
| Total lag | Sum of valid partition lag over all 60 partitions | `total_lag`; whole-run time-weighted mean, sampled peak and coverage; lag plot |
| Mean partition lag | Total lag divided by 60 at each snapshot | `mean_partition_lag`; max/mean partition-lag plot |
| Maximum partition lag | Largest partition lag at a snapshot | `max_partition_lag`; max/mean partition-lag plot |
| Processing backlog | Broker high offset minus application completion frontier, summed over partitions | `processing_backlog`; kept distinct from returned-position lag |
| Lag skew ratio | Maximum partition lag / mean partition lag; zero when all lag is zero | `skew`; snapshot and 15-sample mean plot |
| Instantaneous growth | Change in total lag or processing backlog / actual elapsed time | Both `growth_offsets_per_second` and `processing_backlog_growth_offsets_per_second` |
| Windowed growth | Endpoint change / elapsed time over 5 valid intervals (10 seconds at the 2-second export step) | Both lag and processing-backlog window growth; growth plot uses processing backlog |
| Window mean backlog and skew | Arithmetic mean over 15 contiguous valid snapshots | `window_mean_backlog`, `window_mean_skew` |
| Hot partitions | Lag > 10 offsets and lag > mean + 1 population standard deviation | `hot`; exact partition IDs retained |
| Persistence score | Fraction of the last 15 valid snapshots in which a partition was hot | `persistence`; exact scores retained |
| Persistently hot partitions | Hot now and persistence >= 0.8 | `persistent_hot`; count plot and exact sets retained |
| Completion mean, p50, p95, p99 | Earliest valid completion minus producer timestamp for distinct acknowledged cohort messages; nearest-rank quantiles | `outcome-summary.json`; completion occurs before commit acknowledgment |
| Processing-start latency and processing duration | Producer-to-processing-start delay; local monotonic application-processing duration | `diagnostic_cohort_latencies` |
| Unfinished fraction | Cohort IDs without completion by the original drain cutoff / acknowledged cohort size | Reported beside latency, including the fixed late cohort |
| Primary deadline-miss fraction | Late completed cohort IDs plus unfinished IDs whose deadlines elapsed, divided by the cohort size | `deadline_miss_fraction`; undefined if clocks are invalid or deadlines remain censored |
| Observed-completion SLO violation rate | Late valid completion attempts occurring during evaluation / valid completion attempts occurring during evaluation | `observed_completion_deadline_miss_fraction`; includes warm-up completions and replays, unlike the primary cohort |
| Input and throughput | Acknowledgments and distinct completions per elapsed time; attempts retained separately | Event-based 10-second plots and whole-run summaries |
| Process CPU and memory | Process CPU-time change / elapsed time; resident memory bytes | Per-consumer plots, process time-weighted means, integrals and coverage in `measurement-audit.json` |
| Requested resources | Requested consumer CPU cores and GiB integrated over evaluation plus drain | Core-minutes and GiB-minutes, with coverage; not actual CPU consumption or money |
| Intervention costs | Processing interruption; acknowledged unfinished work at transition boundaries and its net change; recovery time | Event-reconstructed costs and raw handover records |
| Recovery | Processing backlog <= 100 offsets for 30 valid seconds, unchanged ownership/nondecreasing offsets, during continuing input | Confirmed time or not observed; 50/200-offset sensitivity retained |
| Correctness and traceability | Admission/completion identity matching, duplicates, offset continuity and handover verification | `handoff-validation.json`, outcome validity, frozen configuration, runtime signatures and hashes |

Window metrics reset after invalid samples, excessive gaps, changed owners or offset resets. Fifteen samples on a two-second grid span 28 seconds between endpoints; this is distinct from five growth intervals spanning 10 seconds and the separate 30-second recovery hold. Hot/persistence thresholds are exploratory diagnostics, not claimed calibrated controller thresholds.

Time-weighted mean **total lag** across a run is different from mean **partition lag** at one instant. Maximum partition lag is different from the peak sampled total lag across a run. The report labels these explicitly.

The 1-second primary completion target is specified in the new frozen configurations; the campaign protocol also prespecifies the stricter 0.5-second sensitivity target. Both use the same admitted cohort and observation cutoff, count overdue unfinished work, and report censored deadlines. Their fractions are unavailable when censoring or invalid completion clocks prevent complete classification. These experimental targets are not calibrated application requirements, and an acceptable violation percentage has not been specified. Existing 99 ms historical violation rates are not relabeled. Timing uses cross-machine timestamps with recorded clock probes; small latency differences cannot be interpreted as precise causal effects without stronger clock validation.

Raw events allow later sensitivity analysis of deadline thresholds, quantiles and fixed cohorts, with the recomputation clearly identified. Prometheus alone cannot reconstruct message identities or recover missing historical snapshots. Application-specific correctness for future stateful or hot-key-splitting workloads remains outside this stateless experiment.
