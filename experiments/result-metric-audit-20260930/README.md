# Intervention metric analysis

These scripts analyze the twelve current intervention trials: eight with twelve hot partitions and four with four hot partitions. They do not run traffic or change the cluster. The balanced calibration has its own analysis entry point.

With the original raw evidence under `results/`:

```bash
python3 experiments/result-metric-audit-20260930/build_report.py --root . --output results/metric-analysis
python3 experiments/result-metric-audit-20260930/build_compact_figures.py --root . --output results/metric-analysis
```

The first command verifies available source hashes, reconciles deadline outcomes and produces metric summaries. The second builds performance, outcome and runtime-state figures. Temporary replay caches stay in the output directory. The committed [metric summary](../../experiment-records/result-metric-audit-20260930/README.md) is a selected publication output, not a complete raw dataset.

The runtime state is reconstructed from recorded observations with explicit gap and ownership rules. It describes diagnostic signals; no adaptive controller ran in these trials. One-second and half-second deadline analyses for the twelve-partition trials are retrospective; the four-partition protocol specified them before execution.

Resource tables and integral plots report both observed and requested CPU in core-minutes. The saved CPU integral remains in core-seconds; conversion divides the unrounded value by 60 before display. Time-series CPU plots continue to show cores.

The twelve-partition outcome table reports completion latency in the order minimum, mean, maximum and p99. Numeric columns are centered, with alternating pale blue rows and a blue header; the LaTeX preamble must load `xcolor` and `colortbl`. Older outcome summaries are replayed from raw evidence when these fields are absent; the replay must agree with the original cohort counts, p99 and deadline results.
