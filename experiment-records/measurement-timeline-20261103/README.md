# Experiment measurement timeline

This schematic documents the measurement periods for the ten-minute mitigation trials: one minute of warm-up, ten minutes of evaluation and two minutes of drain. Times are relative to evaluation start. Activation and recovery positions are illustrative, not experimental observations.

The figure distinguishes whole-run evaluation-message latency and unfinished work from evaluation-only throughput and monitoring. Resource totals include evaluation and drain. It also shows rolling windows, pre-action arrival-rate estimation, message age and proposed post-activation and post-recovery comparisons. Dashed proposed windows do not assert that every completed trial reports those comparisons. Effective consumer capacity is estimated separately in eligible calibration intervals.

Regenerate from the repository root:

```bash
python3 experiments/result-metric-audit-20260930/plot_metric_timeline.py experiment-records/measurement-timeline-20261103
```

The editable SVG and PNG come from the same Matplotlib source. This is a documentation update; it adds no performance trials and does not change recorded outcomes.
