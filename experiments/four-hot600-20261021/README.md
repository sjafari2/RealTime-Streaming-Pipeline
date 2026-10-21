# Four high-input partitions at 600 messages/s

This comparison follows balanced capacity calibration `run-20261021-055708`. It directs 80% of 600 aggregate messages/s to four of sixty partitions, initially owned by Consumer 2. It is a new deployment block; the earlier 700 messages/s results remain separate.

Two runs test each response: retain three consumers, redistribute within three, scale to six with targeted redistribution, and scale to six with Kafka cooperative-sticky assignment. The second run reverses condition order. Seeds 81 and 82 identify the two workload schedules. Each trial has one minute of warm-up, ten minutes of evaluation and two minutes of drain. Actions are scheduled at evaluation second 60.

Original producer and consumer pod identities, nodes, resource requests and partition ownership are verified before traffic. All six starting client pods remain separate within the namespace; unrelated workloads can still share their nodes. Targeted assignment gives hot-partition counts 2/1/1 with three consumers and 1/1/1/1/0/0 with six. It does not optimize assignments using measured machine capacity.

## Measurements around actions

- Before: evaluation seconds 30–60.
- After activation: verified action activation through evaluation second 600. Keep 3 uses second 60 as its reference.
- Evaluation end: seconds 570–600.
- Plots: trailing thirty-second completion windows, every two seconds, alongside lag and resource observations.

Completion throughput counts distinct earliest completions, including warm-up work finishing in the interval. Window latency uses acknowledged evaluation messages completed in that interval. Mean and nearest-rank p99 come from individual latencies, never averages of percentiles. Pending messages accompany each window. Whole-run cohort latency and unfinished outcomes retain the fixed drain cutoff.

Raw event timestamps, acknowledgments, processing completions, ownership and transition journals, two-second Prometheus exports, process CPU/RSS, requested resources and coverage are retained. Before-and-after signal means use equally weighted valid samples. Missing monitoring is reported as unavailable, not zero. Resource integrals remain distinct from those arithmetic signal means.

## Execution

Preview the design:

```bash
python3 experiments/four-condition-20260929/run.py --aggregate-rate 600 --hot-partitions 4 --slo-ms 1000
```

Execution adds `--execute --monitoring-gate /path/to/passed-monitoring-gap-verification.json`. Empty-topic capture, matching restart and native scaling validation precede performance trials. Technical checks are not counted among the eight trials. Every valid performance outcome is retained.

[Prespecified protocol](protocol.json)

## Completed results and offline analysis

The [eight completed trials](../../experiment-records/four-hot600-20261021/README.md) report all valid outcomes. From the repository root, with the retained raw campaign and run directories restored:

```bash
python3 experiments/four-condition-20260929/summarize.py \
  results/four-condition-20261021-071240 results/four-hot600-20261021-analysis
python3 experiments/four-condition-20260929/analyze_action_windows.py \
  results/four-hot600-20261021-analysis/comparison.json \
  --output results/four-hot600-20261021-analysis
python3 experiments/four-hot600-20261021/analyze_signal_windows.py \
  results/four-hot600-20261021-analysis/comparison.json \
  results/four-hot600-20261021-analysis/action-window-performance.json \
  --campaign results/four-condition-20261021-071240 \
  --output results/four-hot600-20261021-analysis/signal-window-performance.json
python3 experiments/four-hot600-20261021/plot_resources.py \
  results/four-hot600-20261021-analysis/comparison.json \
  --repository . --output results/four-hot600-20261021-analysis/figures
```

Analysis does not start cluster workloads. Large event files must be available locally before replay; a cloud placeholder is not a readable local copy.
