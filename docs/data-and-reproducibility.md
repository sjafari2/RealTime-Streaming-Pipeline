# Data availability and reproducible analysis

I publish the experiment configurations, analysis code, compact result summaries, and available evidence hashes with this repository. The recorded results describe synthetic Kafka workloads; they are not measurements of a deployed biomedical or fire-detection application.

## Published material

| Location | Contents |
| --- | --- |
| `EXPERIMENT_REGISTER.md` | Summary of completed comparisons and planned work |
| `experiment-records/paired-table-20260914/paired-results-data.json` | Run identifiers and per-trial results for the 24 performance trials |
| `experiment-records/campaign-20260912/` | Earlier campaign summaries, comparisons, and calibration records |
| `experiment-records/repetitions-20260913/` | Later repeated comparisons, plots, and ownership reviews |
| `experiments/` | Dated workload configurations and execution protocols; some remain unexecuted |
| `python-scripts/` | Outcome reconciliation, lag, resource, comparison, and repetition analysis |

Each comparison contains two separate trials: keep three consumers and scheduled scaling from three to six. A Run 1 or Run 2 label identifies a comparison, not two phases of a single trial. The later eight trials checked matching starting ownership and original-consumer placement; the earlier sixteen did not require matching starts.

## Raw evidence availability

Full per-message evidence and monitoring exports are not distributed as a complete public raw-data package in this repository. They are retained separately from Git. The public JSON summaries support inspection of reported values and comparisons, but they cannot independently reconstruct a latency distribution without the underlying message records. Historical absolute paths inside evidence files describe the original execution environment; they are not paths that another researcher must create.

Researchers seeking a raw-evidence package can open a repository issue identifying the required run IDs and intended analysis. Availability and transfer arrangements must be confirmed; no public download or permanent external archive is currently promised. Raw archives require checksum verification and preservation separately from source history.

## Collected run format

A complete local run directory normally contains:

- `manifest.json` and `pipeline-configmap.yaml`: run identity, timing boundaries, intervention, and frozen configuration.
- `producer/` and `consumer/`: per-process event logs (`events.jsonl` or compressed equivalents), final status, and final metric snapshots.
- `resource-history.jsonl` and `intervention-events.jsonl`: sampled requests and action observations.
- Monitoring exports and generated `outcome-summary.json`, `lag-summary.json`, and `execution-summary.json`, where collection succeeded.

File availability and measurement coverage are checked per run. A missing monitoring interval is not zero backlog, and an unfinished message has no observed completion latency at the cutoff.

## Recomputing outcomes

The following commands run from the repository root in a Python environment with the documented dependencies. `RUN_ID` and other capitalized run labels are placeholders for collected directories. Preserve an untouched raw-evidence copy before regenerating summaries.

```bash
python3 python-scripts/evaluate_run.py results/RUN_ID
python3 python-scripts/summarize_runs.py results/RUN_A results/RUN_B --output results/recomputed-summary.json
```

The evaluator writes the selected run's outcome summary. The repetition analyzer writes outside its input run directories and groups compatible configurations; it does not automatically treat different interventions as repetitions. A treatment comparison uses the reviewed comparison plan and `compare_runs.py`, documented in the runtime guide.

Completion p99 is computed from distinct acknowledged evaluation messages that finish application processing by the drain cutoff. It is not the mean of rolling p99 values. Unfinished percentage uses all admitted evaluation messages as its denominator. Requested consumer CPU-time is separate from measured CPU usage and whole-cluster cost. Exact definitions and coverage rules are in [Metric definitions](metric-definitions.md).

## Reusing the software and reporting results

A new deployment requires cluster access, compatible images and packages, configured volumes and services, and readiness checks. The [Nautilus setup](nautilus-measurement-update.md) and [runtime guide](runtime-and-data-flow.md) describe those prerequisites and run commands. New experiments should record their own source revision, configuration, placement, timing, admission counts, and measurement coverage. Report observed outcomes separately from proposed mechanisms and application benefits.
