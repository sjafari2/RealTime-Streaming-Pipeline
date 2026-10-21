# Performance before intervention and at evaluation end

Retrospective completion-window diagnostic for the eight twelve-high-input-partition trials; not a replacement for whole-evaluation production-cohort outcomes.

Throughput counts earliest distinct completions in [left,right), including warm-up records; p99 uses individual latencies of acknowledged evaluation-born messages whose earliest completion falls in that interval. Nearest rank; no averaging of percentiles. Completion precedes commit acknowledgment.

Before: evaluation seconds [30,60). Evaluation end: [570,600), before drain. The same windows apply to Keep 3. Rolling traces use trailing 30-second windows at 2-second steps, plotted at the window end.

| Response | Run | Before throughput (msg/s) | End throughput (msg/s) | Before p99 (s) | End p99 (s) | Pending evaluation messages, before / end |
|---|---:|---:|---:|---:|---:|---:|
| Keep 3 | 1 | 423.800 | 427.733 | 57.137 | 313.206 | 33,419 / 182,654 |
| Keep 3 | 2 | 420.233 | 430.100 | 58.064 | 313.977 | 33,875 / 183,386 |
| Redistribute 3 | 1 | 419.667 | 700.433 | 57.357 | 0.179 | 33,513 / 54 |
| Redistribute 3 | 2 | 416.933 | 699.800 | 0.231 | 0.149 | 34,967 / 72 |
| Targeted scale 6 | 1 | 421.133 | 700.033 | 57.295 | 0.472 | 33,506 / 65 |
| Targeted scale 6 | 2 | 408.767 | 699.700 | 0.167 | 0.244 | 34,962 / 77 |
| Kafka scale 6 | 1 | 418.500 | 699.933 | 57.470 | 0.262 | 33,587 / 70 |
| Kafka scale 6 | 2 | 407.167 | 699.967 | 59.564 | 0.198 | 34,786 / 78 |

Only completed evaluation records contribute to window p99. Pending records are reported alongside it. Latency uses original unadjusted producer and consumer clocks, so small differences require caution. Later recovery must not be inferred from p99 alone.

All event and summary hashes were checked against the published comparison. Admission, completion, unfinished, whole-run p99 and throughput were replayed and matched before calculating windows.

![Completion throughput, p99 and pending work](figures/twelve-partition-action-windows.png)

Reproduce from the retained raw evidence at the repository root:

```bash
python3 experiments/four-condition-20260929/analyze_action_windows.py \
  experiment-records/four-condition-20260929/comparison.json \
  --output /path/to/analysis-output
```

The output directory contains the numerical record, table rows and PNG/PDF plots. No cluster access or new performance trial is required.
