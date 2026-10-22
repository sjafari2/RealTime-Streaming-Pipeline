# Four high-input partitions at two aggregate rates

Twelve distinct trials shown together, not pooled. Historical 700 and revised 600 messages/s are different deployment configurations.

| Rate (msg/s) | Response | Run | Min (s) | Mean (s) | Max (s) | p99 (s) | Unfinished (%) | SLO violations (%) |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 600 | Keep 3 | 1 | 0.008 | 166.90 | 323.30 | 309.29 | 26.92 | 85.66 |
| 600 | Keep 3 | 2 | 0.009 | 172.24 | 385.71 | 378.35 | 37.57 | 85.70 |
| 600 | Redistribute 3 | 1 | 0.009 | 17.48 | 112.86 | 91.82 | 0.00 | 38.76 |
| 600 | Redistribute 3 | 2 | 0.008 | 12.83 | 119.70 | 83.29 | 0.00 | 31.32 |
| 700 | Redistribute 3 | 1 | 0.009 | 10.75 | 98.78 | 84.50 | 0.00 | 26.63 |
| 700 | Redistribute 3 | 2 | 0.009 | 9.93 | 90.89 | 79.28 | 0.00 | 25.82 |
| 600 | Targeted scale 6 | 1 | 0.008 | 21.87 | 150.31 | 114.08 | 0.00 | 36.75 |
| 600 | Targeted scale 6 | 2 | 0.007 | 19.25 | 146.99 | 109.42 | 0.00 | 33.43 |
| 700 | Targeted scale 6 | 1 | 0.008 | 19.33 | 114.27 | 110.19 | 0.00 | 35.23 |
| 700 | Targeted scale 6 | 2 | 0.008 | 20.02 | 123.40 | 112.94 | 0.00 | 34.72 |
| 600 | Kafka scale 6 | 1 | 0.007 | 15.46 | 87.66 | 75.59 | 0.00 | 44.35 |
| 600 | Kafka scale 6 | 2 | 0.007 | 35.65 | 100.66 | 90.28 | 0.00 | 66.75 |

The [combined tables](tables.tex) include all twelve trials in outcomes, observed/requested resources, backlog/skew, intervention cost and completion-window performance. Blue denotes 700 messages/s and amber denotes 600 messages/s. The rate is also printed on every row. Missing interventions are not invented for the historical block.

The newer deployment used different client resource settings, producer-consumer placement and producer seed initialization. Compare responses within each deployment; differences across rates cannot be attributed to rate alone. The proposal reports each trial once in each metric table and places the 600 messages/s plots after the historical 700 messages/s plots in the shared four-partition section.

Latency follows distinct acknowledged evaluation messages through the drain cutoff; completion precedes commit acknowledgment. Window p99 uses only evaluation messages completed within each window. Throughput includes warm-up work completed during evaluation. Means of backlog and skew are arithmetic means of valid individual samples, matching the existing four-partition table. Resource integrals cover evaluation plus drain. Missing observations are not zero.

Historical message records were replayed to obtain the previously omitted minimum/maximum latency and completion-window values, with original counts, whole-run p99 and throughput verified. The [historical completion-window record](historical-completion-windows.json) and [combined numerical record](comparison.json) retain measurement definitions and source hashes.

## Relative changes

Throughput and resource-request increases = 100 × (response − reference) / reference. Backlog, p99 and SLO-rate reductions = 100 × (reference − response) / reference. Positive request increases mean added resource cost; positive throughput increases and positive reductions mean performance improvements. A zero or unavailable reference is not assigned a percentage. Values are calculated before rounding, without averaging run percentiles.

Backlog means the arithmetic mean of valid evaluation total-backlog samples. SLO is the full-cohort one-second violation rate; relative reduction differs from percentage-point decrease. Cost is requested consumer CPU and memory over evaluation plus drain, not actual use or a monetary bill.

### 600 messages/s: each intervention relative to Keep 3 with the same run number

| Response | Run | Throughput increase (%) | Mean backlog reduction (%) | p99 reduction (%) | SLO-rate reduction (%) | CPU-request increase (%) | Memory-request increase (%) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Redistribute 3 | 1 | 57.4 | 88.3 | 70.3 | 54.8 | 0.0 | 0.0 |
| Redistribute 3 | 2 | 80.1 | 91.8 | 78.0 | 63.4 | 0.0 | 0.0 |
| Targeted scale 6 | 1 | 59.6 | 85.3 | 63.1 | 57.1 | 89.8 | 89.8 |
| Targeted scale 6 | 2 | 80.2 | 88.1 | 71.1 | 61.0 | 89.6 | 89.6 |
| Kafka scale 6 | 1 | 59.5 | 88.3 | 75.6 | 48.2 | 90.3 | 90.3 |
| Kafka scale 6 | 2 | 72.1 | 76.6 | 76.1 | 22.1 | 90.1 | 90.1 |

Every percentage uses the Keep 3 trial at 600 messages/s with the same run number. Historical 700 messages/s trials are not compared with that baseline. Two runs do not establish statistical significance.

## Reproduce the tables

The historical replay reads the separately retained raw message evidence and verifies its recorded hashes. The comparison builder reads the existing campaign analysis and checks the newer manifest and lag-summary hashes. The full newer analysis remains in local results storage; the committed records preserve the values used here and their hashes.

```bash
python3 experiments/four-condition-20260929/analyze_action_windows.py \
  experiment-records/four-hot-partitions-20260930/comparison.json \
  --output results/four-hot700-window-replay
python3 experiments/four-hot600-20261021/compare_rates.py \
  --historical-windows results/four-hot700-window-replay/action-window-performance.json \
  --current-comparison results/four-hot600-20261021-analysis/comparison.json \
  --output results/four-hot-rate-comparison
```

Use `--current-raw-root` if the newer raw run directories are stored outside the repository. The published historical window file omits only the long rolling series; it retains interval metrics, replay checks, measurement definitions and source hashes.
