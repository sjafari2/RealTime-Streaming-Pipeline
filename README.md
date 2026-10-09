# RealTime Streaming Pipeline

I developed this Kafka research platform to study how workload skew, partition ownership and consumer capacity affect stream processing. The study compares scaling, targeted partition redistribution and their costs, and provides a foundation for evaluating hot-key splitting, combinations of interventions and execution order.

The implementation uses Python producers and consumers on Kubernetes, with Prometheus and Grafana for monitoring. Experiments retain message identities and completion records alongside the monitoring data, so latency can be reported with unfinished work.

## Research and results

The current result set contains **24 performance trials** used in the main proposal:

| Experiment | Trials | Main observation |
|---|---:|---|
| Balanced input, 600–1,500 messages/s | 12 | Backlog remained bounded at 600–800 messages/s over the observed interval and grew at higher rates. |
| 80% of input across 12 of 60 partitions | 8 | Redistribution and both scaling responses completed all evaluation messages; keeping the starting assignment left about 35% unfinished. |
| 80% of input across 4 of 60 partitions | 4 | Redistribution within three consumers recorded lower whole-cohort p99 than targeted scaling to six in both runs. |

The skew comparisons use **700 messages/s in total across three producers**. Each condition has two runs. These are scheduled interventions on a synthetic workload; the adaptive controller and hot-key splitting remain research work.

[Results, tables and plots](experiment-records/README.md) · [Research questions](docs/research-questions.md) · [Methodology](docs/experiment-methodology.md) · [Metric definitions](docs/metric-definitions.md)

## Running the pipeline

A configured Kubernetes deployment, shared volumes, compatible images and monitoring are required. The [deployment guide](docs/nautilus-measurement-update.md) covers these prerequisites. From the repository root:

```bash
# One experiment using the shared configuration.
bash my-shell/save-run.sh

# Two runs of the same configuration.
bash my-shell/run_pipeline.sh --repetitions 2
```

The coordinator prepares fresh topics, checks readiness, runs warm-up, evaluation and drain, then collects and analyzes the evidence. Configuration changes are made between runs in `/config/pipeline-configmap.yaml`. The local [template](src/pipeline-configmap.yaml) does not update a running deployment automatically.

The [experiment protocols](experiments/README.md) provide the specific commands and settings for the published comparisons. The [runtime guide](docs/runtime-and-data-flow.md) explains the file relationships and output format.

## Repository layout

| Directory | Contents |
|---|---|
| `src/` | Producer, consumer and shared runtime |
| `my-shell/` | Run coordination, deployment helpers and source synchronization |
| `python-scripts/` | Outcome, lag, resource and comparison analysis |
| `experiments/` | Current experiment protocols and analysis entry points |
| `experiment-records/` | Published summaries, provenance and selected plots |
| `k8s/`, `dockerfiles_confluent/` | Infrastructure and image definitions |
| `grafana/` | Monitoring dashboards |
| `tests/` | Local tests for runtime and measurement behavior |
| `docs/` | Research and operational documentation |

Large raw results are stored separately from Git. Public summaries do not replace the per-message evidence needed to recompute latency distributions; see [data availability](docs/data-and-reproducibility.md).

## Development

Changes are prepared on **`development`** and merged into **`main`** after review. Earlier campaigns and legacy implementations are preserved on the [archive branch](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008). This keeps the active tree focused while retaining the original history and outcomes.

```bash
python3 -m pip install -r dockerfiles_confluent/runtime-requirements.txt pytest numpy matplotlib
python3 -m pytest -q
```

See [contributing](CONTRIBUTING.md) for the branch workflow and [limitations and next steps](docs/limitations-and-roadmap.md) for the remaining research.
