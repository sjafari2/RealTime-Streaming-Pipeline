# Hot-partition ownership at a balanced-stable reference rate

These two calibration trials examine whether concentrating hot partitions on one consumer creates sustained backlog at a rate that previously had low balanced-workload lag. Each trial uses **700 aggregate messages/s, three consumers, and 60 partitions**. The same partition IDs 0–11 receive 80% of the input; the remaining 48 receive 20%.

| Trial | Hot partitions per consumer (0 / 1 / 2) | Total partitions per consumer | Expected input per consumer (msg/s) |
|---|---|---|---|
| Distributed | 4 / 4 / 4 | 20 / 20 / 20 | 233.33 / 233.33 / 233.33 |
| Concentrated | 12 / 0 / 0 | 20 / 20 / 20 | 583.33 / 58.33 / 58.33 |

Expected rates include cold-partition traffic. They are probabilistic workload targets; delivered counts are measured. The producer seed is 71 in both trials, with 2000 SHA-256 iterations per message, no added processing sleep, and 100-byte payloads. Both trials use the same explicit assignment mechanism and synchronous completion-based commits. The exact ownership maps are stored in the two YAML files and verified before traffic is released.

Each trial lasts **eight minutes: one minute warm-up, five minutes evaluation, two minutes drain**. Warm-up messages are excluded from cohort p99 and unfinished percentages, but their remaining work is retained in the pipeline. Completion is measured at the end of application processing, before commit acknowledgment. The runner retains producer acknowledgments, distinct completion outcomes, lag and backlog traces, throughput, CPU and memory measurements, configuration and resource records.

No consumer scaling or ownership handoff occurs during these trials. These are separate initial-layout calibrations, once each, not repeated comparative evidence of a live redistribution policy. The existing redistribution pilot still requires live handoff correctness tests before intervention trials. Machine placement is recorded, without introducing a new node-pinning constraint. Shared-machine variability, fixed trial order and one trial per layout limit causal and long-term stability claims.

From the repository root, preview the reviewed design:

```bash
python3 experiments/hot-ownership-20260927/run.py
```

With the Nautilus context authenticated, execute both trials in order:

```bash
python3 -u experiments/hot-ownership-20260927/run.py --execute
```

The runner requires committed tracked changes, refuses an active managed experiment, pauses competing HPA decisions for this invocation, checks source hashes and monitoring, and stops the campaign if collection or measurement checks fail. It saves campaign status and original settings under `results/hot-ownership-<timestamp>/`, and run evidence under `results/run-<timestamp>/`. Applications stop after the block; the saved shared configuration is restored only if it was not changed externally. Existing topics and earlier evidence are preserved.

## Completed calibration

Both trials completed on 27 September 2026. See the [results, interpretation and plots](../../experiment-records/hot-ownership-20260927/README.md). The execution revision and evidence hashes are retained in the comparison record.

With the separately retained raw evidence available, regenerate the tables and figures using Python with NumPy and Matplotlib:

```bash
python3 experiments/hot-ownership-20260927/summarize.py \
  results/hot-ownership-20260927-020125 \
  experiment-records/hot-ownership-20260927
```

The summary tool validates both frozen configurations, exact starting layouts, completed-run checks and observed ownership before producing the plots. It retains missing samples as gaps and reports whole-cohort p99 separately from the rolling backlog-growth trace.

## Additional calibration: concentration on Consumer 2

The follow-up keeps the same workload and eight-minute schedule, but assigns all twelve hot partitions to Consumer 2. Each consumer still owns 20 total partitions. Expected input, including cold traffic, is 58.33 / 58.33 / 583.33 messages/s on Consumers 0 / 1 / 2. This checks the effect of the selected consumer's processing capacity. It remains a fixed initial-layout calibration, with no mid-run handoff or scaling.

Preview this trial only:

```bash
python3 experiments/hot-ownership-20260927/run.py --layouts concentrated-c2
```

Execute this trial only:

```bash
python3 -u experiments/hot-ownership-20260927/run.py --layouts concentrated-c2 --execute
```

The default selection remains the original distributed and Consumer 0 layouts. Explicit selection prevents accidentally rerunning them when only the Consumer 2 calibration is requested. The Consumer 2 layout is exploratory calibration selected after observing the earlier two layouts; it is not a prespecified repeated mitigation comparison.

To test Consumer 1 under the same concentration, select `--layouts concentrated-c1`. The follow-up block runs Consumer 2 and then Consumer 1, once each:

```bash
python3 -u experiments/hot-ownership-20260927/run.py \
  --layouts concentrated-c2 concentrated-c1 --execute
```

The trials run sequentially because they share the runtime configuration and Kafka application pods. This block does not repeat the completed distributed or Consumer 0 trials.
