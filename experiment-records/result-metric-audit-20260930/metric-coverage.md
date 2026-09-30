# Measurement coverage

| Metric family | Evidence | Interpretation |
|---|---|---|
| Partition lag and total lag | Main lag plots, diagnostic figure and lag-summary tables; older five-minute counterparts in the appendix. | Offsets, not guaranteed record counts. |
| Processing backlog and growth | Main backlog-growth plots and acknowledged-unfinished plots; growth is also retained for consumer-position lag. | Completion frontier and returned-record position have different definitions. |
| Maximum and mean partition lag | Bottom row of each diagnostic figure and sampled-peak tables. | Maximum and mean are taken across partitions at each valid observation. |
| Lag skew and window-mean skew | Runtime-state row shows window-mean skew; the summary table and separate diagnostic plot retain raw skew. | High relative skew can coexist with small absolute lag. |
| Hot set, persistence score and persistent-hot set | Top diagnostic row, detector settings and interpretation of early versus late observations. | Exploratory diagnostic, not a calibrated controller trigger. |
| Window-mean backlog and runtime state | Runtime-state figure and timestamped CSV/JSON, including persistent-hot partition IDs. | Reconstructed from retained observations; no adaptive controller was evaluated. |
| Completion mean, p50, p95 and p99 | Outcome and latency-detail tables for each block. | Each quantile uses completed evaluation-born messages, not averaged rolling quantiles. |
| Processing-start latency and processing duration | Latency-detail tables and separation of queued delay from synthetic work. | Processing duration excludes evidence writing and commit bookkeeping. |
| Unfinished work and completion deadlines | Unfinished percentage and 1-second cohort deadline misses in main tables; 0.5-second sensitivity in appendix. | Overdue unfinished messages count as misses; they have no fabricated completion latency. |
| Observed-completion violation rate | Separate latency-detail column at 1 second. | Completion-attempt population during evaluation differs from the admitted cohort. |
| Input, useful throughput and attempt throughput | Main throughput plots and outcome tables; admitted rate and replay checks in text. | Attempts and distinct completions agree only when no duplicate completions occur. |
| Actual CPU and resident memory | Main process traces and observed resource integrals. | Consumer processes only; no full-cluster energy or monetary-cost claim. |
| Requested CPU and memory cost | Main resource tables, integrated over evaluation plus drain; detailed resource-cost figures retained with records. | Partial request coverage is unavailable for full-run comparison, not zero. |
| Interruption, transition backlog and recovery | Main intervention-cost tables, with operational endpoints identified. | Recovery is first threshold confirmation while input continues; not permanent stability. |
| Correctness and measurement quality | Identity reconciliation, handover validation, clock limitations and explicit coverage discussion. | Synthetic records do not prove exactly-once business effects at a durable external sink. |
| Controller overhead, splitting and state-transfer cost | Explicitly identified as unevaluated future measurements. | No adaptive controller, hot-key splitting or stateful transfer was evaluated in these trials. |
