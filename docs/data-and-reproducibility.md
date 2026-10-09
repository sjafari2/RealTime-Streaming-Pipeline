# Data and reproducibility

I publish the configurations, analysis code, compact summaries and available evidence hashes for the [24 current trials](../experiment-records/README.md). These are synthetic Kafka experiments. Earlier campaigns and superseded reports are preserved on the [archive branch](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008).

## Public results

Each result group contains a short README and its numerical comparison file. The intervention metric summary includes the reported one-second deadline analysis and resource coverage; selected figures show performance, resources, outcomes and reconstructed runtime signals. Run identifiers and original execution revisions remain in the evidence records.

Generated JSON, CSV and plots are analysis outputs, not a second manually maintained report. A human-readable summary needs only the design, main findings, limitations and links to those outputs. The [experiment index](../experiments/README.md) identifies the scripts that produce them.

## Raw evidence

Full per-message logs and monitoring exports are stored separately under ignored `results/` directories and independent backups. The repository summaries cannot reconstruct an entire latency distribution without those records. An issue identifying the required run IDs can be used to request a raw-evidence package; a public download or permanent external archive is not currently promised.

A collected run normally contains:

| File or directory | Evidence |
|---|---|
| `manifest.json`, `pipeline-configmap.yaml` | Run identity, timing, configuration, intervention and source signatures |
| `producer/`, `consumer/` | Message events, ownership events, final status and metric snapshots |
| `prometheus.json` | Run-scoped monitoring with labels and unavailable observations |
| `resource-history.jsonl` | Sampled consumer resource requests and pod history |
| `intervention-events.jsonl`, `explicit-handoff-events.jsonl` | Action and handover observations, when applicable |
| `outcome-summary.json`, `lag-summary.json`, `execution-summary.json` | Derived outcomes and coverage |

File availability is checked per trial. Historical absolute paths describe the original execution environment, not directories that another researcher must recreate.

## Reanalysis

With collected raw evidence under `results/`, the following commands recompute individual and repeated-run summaries:

```bash
python3 python-scripts/evaluate_run.py results/RUN_ID
python3 python-scripts/summarize_runs.py results/RUN_A results/RUN_B --output results/recomputed-summary.json
```

`RUN_ID`, `RUN_A` and `RUN_B` are placeholders. Reanalysis uses a copy of the raw evidence; the original files and checksums remain preserved. Different configurations and interventions are not automatically treated as repetitions. See [metric definitions](metric-definitions.md) for cohort p99, pooled quantiles, unfinished work and coverage rules.

Source history, compact result records and raw evidence require separate backups. A Git summary is not a backup of its underlying measurements.
