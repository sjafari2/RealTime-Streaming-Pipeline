# Hot-partition ownership at a balanced-stable reference rate

Four calibration layouts examine whether concentrating hot partitions on a particular consumer creates sustained backlog at a rate that previously had low balanced-workload lag. Each trial uses **700 aggregate messages/s, three consumers, and 60 partitions**. The same partition IDs 0–11 receive 80% of the input; the remaining 48 receive 20%.

| Layout | Hot partitions per consumer (0 / 1 / 2) | Total partitions per consumer | Expected input per consumer (msg/s) |
|---|---|---|---|
| Distributed | 4 / 4 / 4 | 20 / 20 / 20 | 233.33 / 233.33 / 233.33 |
| All hot partitions on Consumer 0 | 12 / 0 / 0 | 20 / 20 / 20 | 583.33 / 58.33 / 58.33 |
| All hot partitions on Consumer 2 | 0 / 0 / 12 | 20 / 20 / 20 | 58.33 / 58.33 / 583.33 |
| All hot partitions on Consumer 1 | 0 / 12 / 0 | 20 / 20 / 20 | 58.33 / 583.33 / 58.33 |

Expected rates include cold-partition traffic. They are probabilistic workload targets; delivered counts are measured. Every layout uses producer seed 71, 2000 SHA-256 iterations per message, no added processing sleep, and 100-byte payloads. All use explicit assignment and synchronous completion-based commits. The exact ownership maps are stored in the YAML files and verified before traffic is released. These are fixed starting layouts; no consumer scaling or ownership handoff occurs during a trial.

Each trial lasts **eight minutes: one minute warm-up, five minutes evaluation, two minutes drain**. Preparation and collection take additional wall-clock time. Warm-up messages are excluded from cohort p99 and unfinished percentages, but their remaining work is retained in the pipeline. Completion is measured at the end of application processing, before commit acknowledgment. The runner retains producer acknowledgments, distinct completion outcomes, lag and backlog traces, throughput, CPU and memory measurements, configuration and resource records.

## Execution

From the repository root, preview the original distributed and Consumer 0 trials:

```bash
python3 experiments/hot-ownership-20260927/run.py
```

With the Nautilus context authenticated, execute that original block:

```bash
python3 -u experiments/hot-ownership-20260927/run.py --execute
```

Explicit selection runs only the requested layouts. Preview the Consumer 2 and Consumer 1 block, then execute it when appropriate:

```bash
python3 experiments/hot-ownership-20260927/run.py \
  --layouts concentrated-c2 concentrated-c1
python3 -u experiments/hot-ownership-20260927/run.py \
  --layouts concentrated-c2 concentrated-c1 --execute
```

Use `--layouts concentrated-c1` or `--layouts concentrated-c2` to select just one of those trials. The default remains the original two layouts. Trials run sequentially because they share the runtime configuration and application pods.

The runner requires committed tracked changes, refuses an active managed experiment, pauses competing HPA decisions for this invocation, checks source hashes and monitoring, and stops the campaign if collection or measurement checks fail. It saves campaign status and original settings under `results/hot-ownership-<timestamp>/`, and run evidence under `results/run-<timestamp>/`. Applications stop after the block; the saved shared configuration is restored only if it was not changed externally. Existing topics and earlier evidence are preserved.

## Completed calibrations and interpretation

All four layouts completed once on 27 September 2026, in the order listed above. The [combined results, interpretation and figures](../../experiment-records/hot-ownership-consumers-20260927/README.md) retain the execution revisions, exact outcomes and evidence hashes. The [original two-trial record](../../experiment-records/hot-ownership-20260927/README.md) remains available separately.

The Consumer 2 and Consumer 1 layouts were selected after the first two observations. At approximately equal input in the distributed layout, Consumer 2 had higher measured CPU use, suggesting less processing headroom. This motivated an exploratory concentration test; it was not a prespecified confirmatory comparison. Shared-machine variability, fixed order and one trial per layout limit causal and long-term stability claims. Machine placement was recorded, without introducing a new node-pinning constraint.

These initial-layout calibrations do not measure the benefit or disruption of live redistribution. The redistribution pilot still requires live handoff correctness tests before intervention comparisons.

## Reproducing the figures

With the separately retained raw evidence available, regenerate all four layouts using Python with NumPy and Matplotlib:

```bash
python3 experiments/hot-ownership-20260927/summarize.py \
  results/hot-ownership-20260927-020125 \
  results/hot-ownership-20260927-024128 \
  experiment-records/hot-ownership-consumers-20260927 \
  --individual-lag concentrated-c2
```

One completed campaign can also be summarized on its own. The optional individual-lag selection adds a full-size view of a selected layout. The summary tool validates frozen configurations, distinct run-specific topics, exact starting layouts, completed-run checks and observed ownership before producing plots. Missing observations remain gaps. Whole-cohort p99 is separate from the rolling backlog-growth trace; lag skew is the maximum partition lag divided by mean partition lag and must be interpreted alongside backlog magnitude and growth.
