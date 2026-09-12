# Kafka pipeline experiments

The active workflow uses a YAML file shared by the producer and consumer pods. I stop the applications before editing it, then run one complete experiment with:

```bash
bash my-shell/save-run.sh
```

The command checks the environment, connects to the existing Prometheus service, starts the run, waits through production/drain/final snapshots, collects evidence and exports metrics, then writes outcome, lag and execution summaries. It needs no interactive answers. To repeat the current shared configuration five times:

```bash
bash my-shell/run_pipeline.sh --repetitions 5
```

For five repetitions at each of several per-producer rates:

```bash
bash my-shell/run_pipeline.sh --rates 1000 2000 3000 --repetitions 5
```

Every run has its own `results/RUN_ID/` folder. Repeated commands also write `results/batches/BATCH_ID/batch-status.json` and, when the batch completes, `repetition-summary.json`. A failure stops the batch and preserves existing outputs. Fresh topics are created for each run; old topics are not automatically deleted. The command prints the actual result paths.

Read [the Nautilus update guide](docs/nautilus-measurement-update.md) before the first run with this version. It explains code synchronization, automatic startup of scaled consumers, Prometheus discovery and the measurements to check on Nautilus.

Aggregate measurements are exported from Prometheus. Compact outcome records and final snapshots on the shared volumes retain information needed to check unfinished messages, duplicates and process termination. `python-scripts/evaluate_run.py` produces the cohort summary.

Local tests:

```bash
python3 -m pip install -r dockerfiles_confluent/runtime-requirements.txt pytest
python3 -m pytest -q tests
```

Read [the runtime and data-flow guide](docs/runtime-and-data-flow.md) for daily commands, timing, outputs, troubleshooting and the relationship between files.

Superseded scripts, duplicate exporters, older analysis tools and the unfinished controller are in [archive/legacy-before-managed-runs](archive/legacy-before-managed-runs/README.md). The archive records original paths and checksums. It is excluded from the active application image sources and test discovery. Historical measurements remain available. Infrastructure manifests and clock diagnostics remain because they serve separate operational purposes.

The current application implements measurement and run management. Optional scheduled no-action and scale-up pilots are available in both entry points. Targeted reassignment and the adaptive selector remain unimplemented. Read [the pilot commands and limits](docs/runtime-and-data-flow.md#10-scheduled-pilots-and-new-results).

Exact measurement definitions and analysis commands: [Metric definitions](docs/metric-definitions.md) and [runtime analysis](docs/runtime-and-data-flow.md#9-analyze-collected-experiments). Whole-run cohort results and pooled repetitions are separate from rolling Grafana attempt metrics.

Git history, backup and commit workflow: [Git history and backups](docs/git-history-and-backups.md).
Small completed-run records are in [experiment-records](experiment-records/README.md).
The [next experiment configuration](experiments/next-run-review/README.md) is prepared for review and has not been applied or run.
