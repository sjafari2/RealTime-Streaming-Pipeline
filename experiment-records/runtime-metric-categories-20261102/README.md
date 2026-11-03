# Runtime metric comparisons

These figures show the 36 skewed-workload trials in the proposal. The twelve balanced calibration trials remain in the separate reference-rate results. No trials were rerun to create these plots.

| Experiment block | Trials | Plot columns |
| --- | ---: | --- |
| Twelve high-input partitions, 700 messages/s | 8 | Run 1 and Run 2 |
| Four high-input partitions, 700 and 600 messages/s | 12 | Separate deployment blocks |
| Smaller and larger starting backlogs, 600 messages/s | 16 | Separate starting-backlog conditions |

The four-partition 700 messages/s block contains only redistribution and targeted scaling. Colors identify responses; solid and dashed lines distinguish runs where both appear in one panel. Curves are not averaged or pooled across deployments.

## Metrics and timing

All traces cover the ten-minute evaluation period. The gray dotted vertical line marks the scheduled action at minute 1. Colored dotted lines mark confirmed activation separately for every intervention run, using the response color. Keep 3 has no activation marker. Active–end continues from each activation line to minute 10; activation is not backlog recovery. Shading identifies seconds 30–60 (Before) and 570–600 (Eval. end). The horizontal throughput reference is the target total input rate.

- **Performance evaluation:** processing backlog Q(t), throughput and window p99 end-to-end latency. Q(t) comes from completion offsets and is not consumer-position lag or the exact number of distinct unfinished evaluation messages. Throughput counts distinct first completions in nonoverlapping 30-second intervals, including warm-up messages completed during evaluation. Window p99 uses evaluation messages completed in that interval, measured from creation to completion. Values are plotted at the interval end. Window p99 excludes drain and is not whole-run p99; it cannot describe messages without a recorded completion.
- **Runtime control signals:** mean processing-backlog growth over 15 valid two-second intervals, mean lag skew over 15 consecutive valid observations, and the number of persistently hot partitions from the saved lag-based classification. Growth uses Q(t); skew and persistence use partition lag. Missing observations and ownership changes break the affected curves until the required window is available again.
- **Mitigation cost:** actual consumer-process CPU use in cores and RSS memory in MiB, summed only when each started consumer process has a fresh observation. These are process measurements, not pod or node totals. Missing components are not counted as zero. Requested-resource integrals, activation delay and processing pauses remain separate measures in the other result figures.

The figures retain the original run-level outcomes. Whole-run latency, unfinished messages, SLO violations, requested resources and intervention costs are not replaced by these time-series metrics. Arrival-rate and capacity findings remain in their existing separate analyses; this plot set does not infer new capacity estimates.

## Reproduce

From the repository root, with Python and Matplotlib installed:

```bash
python3 experiments/result-metric-audit-20260930/plot_metric_categories.py \
  --input experiment-records/runtime-metric-categories-20261102/metric-category-plot-data.json \
  --output /tmp/runtime-metric-category-plots
```

This command renders all nine figures from compact verified observations without downloading raw logs. The output includes PDF, PNG and SVG versions. The saved compact data retain comparison-file hashes, run IDs, message-evidence hashes and monitoring-source hashes. Original source evidence remains separately backed up; these summaries are not a raw-data backup.

## Validation

All 36 run identities and campaign counts were checked. Source manifests, lag summaries and Prometheus records matched the recorded evidence hashes. Saved completion-window reports were matched to message-evidence hashes and run outcomes; the four-partition 700 messages/s windows were reconstructed from original message events and checked against acknowledged counts, completed counts and whole-run p99. Integrated window throughput matched the original evaluation throughput for every trial. All completion intervals span exactly 30 seconds, with endpoints from second 30 through 600. Plot bounds include every valid value, and missing values remain gaps.

## Confirmed activation markers

The 28 intervention boundaries are verified against the original evidence. Targeted actions use the `active_verified` handover event and its complete partition map. Normal Kafka scaling uses the saved six-consumer ownership map confirmed unchanged for ten seconds. The eight Keep 3 runs retain only the scheduled reference. `activation-validation.json` records all boundaries and evidence hashes. Every original metric series remains unchanged.

To recheck the underlying records when reproducing the plots, add `--repository /path/to/RealTime-Streaming-Pipeline` to the command above. Without that option, the renderer uses the verified boundaries stored with the compact plot data.
