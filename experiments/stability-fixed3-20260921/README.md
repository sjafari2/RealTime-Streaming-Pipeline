# Balanced-rate calibration

This protocol tests balanced input with three fixed consumers and no mitigation. The published set contains two trials at each aggregate rate: 600, 700, 800, 900, 1,200 and 1,500 messages/s. Three producers each target one third of the selected rate, retaining fractional values.

Each trial uses 60 partitions, 2,000 SHA-256 iterations per message, no added sleep, seed 71, and fresh topics. Timing is one minute warm-up, twenty minutes evaluation and two minutes drain: 23 minutes in total. Warm-up work can remain queued even though warm-up messages are excluded from the outcome cohort.

Preview the complete design:

```bash
python3 experiments/stability-fixed3-20260921/run.py --rates 600 700 800 900 1200 1500 --repetitions 2
```

Adding `--execute` starts the selected trials. A smaller calibration uses a subset of `--rates`. The runner stops on failure and restores the previous shared configuration. Campaign status and trial paths are saved under `results/stability-fixed3-*`.

The [base configuration](base-config.yaml) and [no-action plan](no-action.json) retain the calibration settings. The [results](../../experiment-records/stability-fixed3-20260921/README.md) report latency, unfinished work and growth. Bounded backlog during the observed interval does not prove indefinite stability.

With the twelve collected raw run directories, `plot_lag_multipanel.py OUTPUT_PREFIX RUN_DIRECTORY...` reproduces the aggregate lag figure. `python-scripts/analyze_stability.py RUN_DIRECTORY` computes per-run growth windows and stability summaries.
