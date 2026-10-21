# Performance before intervention and at evaluation end

Completion-window diagnostic for eight trials with 4 high-input partitions at 600 messages/s; separate from whole-evaluation production-cohort outcomes.

Throughput counts earliest distinct completions in [left,right), including warm-up records; p99 uses individual latencies of acknowledged evaluation-born messages whose earliest completion falls in that interval. Nearest rank; no averaging of percentiles. Completion precedes commit acknowledgment.

Before: evaluation seconds [30,60). Evaluation end: [570,600). Post-action: [verified activation,600), using explicit active_verified or a native six-owner map confirmed unchanged for ten seconds; Keep 3 uses second 60 as its no-action reference. Activation is not backlog recovery. No window includes drain. Rolling traces use trailing 30-second windows at 2-second steps, plotted at the window end.

| Response | Run | Before throughput (msg/s) | End throughput (msg/s) | Before p99 (s) | End p99 (s) | Pending evaluation messages, before / end |
|---|---:|---:|---:|---:|---:|---:|
| Keep 3 | 1 | 342.467 | 443.500 | 59.566 | 272.393 | 30,686 / 140,043 |
| Keep 3 | 2 | 342.233 | 345.700 | 0.228 | 324.890 | 30,723 / 167,208 |
| Redistribute 3 | 1 | 346.600 | 600.167 | 49.854 | 0.399 | 25,786 / 61 |
| Redistribute 3 | 2 | 347.200 | 600.400 | 59.134 | 0.449 | 30,334 / 54 |
| Targeted scale 6 | 1 | 341.200 | 600.467 | 0.352 | 0.327 | 30,887 / 57 |
| Targeted scale 6 | 2 | 346.733 | 600.367 | 59.322 | 0.222 | 30,423 / 61 |
| Kafka scale 6 | 1 | 344.867 | 600.333 | 59.777 | 0.519 | 30,784 / 37 |
| Kafka scale 6 | 2 | 348.467 | 607.833 | 58.783 | 44.733 | 30,136 / 16,745 |

## From verified activation to evaluation end

| Response | Run | Start (evaluation s) | Duration (s) | Throughput (msg/s) | Completion p99 (s) | Evaluation completions for p99 |
|---|---:|---:|---:|---:|---:|---:|
| Keep 3 | 1 | 60.000 | 540.000 | 397.487 | 270.645 | 214,643 |
| Keep 3 | 2 | 60.000 | 540.000 | 347.304 | 321.129 | 187,515 |
| Redistribute 3 | 1 | 131.450 | 468.550 | 699.437 | 91.907 | 327,721 |
| Redistribute 3 | 2 | 97.529 | 502.471 | 684.609 | 82.676 | 343,996 |
| Targeted scale 6 | 1 | 157.792 | 442.208 | 720.304 | 106.881 | 318,524 |
| Targeted scale 6 | 2 | 151.792 | 448.208 | 709.269 | 103.485 | 317,900 |
| Kafka scale 6 | 1 | 96.000 | 504.000 | 648.055 | 73.476 | 326,620 |
| Kafka scale 6 | 2 | 98.000 | 502.000 | 625.350 | 91.254 | 313,926 |

Only completed evaluation records contribute to window latency and its deadline-miss fraction; these are separate from full-cohort deadline outcomes. Pending records are reported alongside it. Post-action durations vary with activation time and exclude the preceding action transition; they do not measure total intervention cost. The interval includes previously queued messages and subsequent backlog recovery. Latency uses original unadjusted producer and consumer clocks, so small differences require caution. Later recovery must not be inferred from p99 alone.

All event and summary hashes were checked against the source comparison. Activation boundaries were checked against the retained handoff journals or lag snapshots and the original transition summary; journal hashes are retained. Admission, completion, unfinished, whole-run p99 and throughput were replayed and matched before calculating windows.

![Completion throughput, p99 and pending work](figures/action-windows.png)

Reproduce from the retained raw evidence at the repository root:

```bash
python3 experiments/four-condition-20260929/analyze_action_windows.py \
  /path/to/comparison.json \
  --output /path/to/analysis-output
```

The output directory contains the numerical record, table rows and PNG/PDF plots. No cluster access or new performance trial is required.
