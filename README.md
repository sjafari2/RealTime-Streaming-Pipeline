# Kafka Skew and Consumer Elasticity

**Experiment progress:** [Open the experiment register](EXPERIMENT_REGISTER.md) for all completed runs, plots, rejected preparations and the next approved step.

A research platform for studying how workload imbalance affects a Kafka stream-processing pipeline, when additional consumers help, and how the cost of changing the configuration should influence mitigation decisions.

This repository supports the PhD research project **Skew-Resilient Kafka: A Lag-Driven Autoscaling Approach**, by **Soheila Jafari Khouzani**, Department of Computer Science, University of New Mexico. It combines Python producers and consumers, Kubernetes deployment resources, managed experiment execution, and reproducible analysis of message completion and partition-level behavior.

**Current implementation:** managed measurements, evidence collection, repeated-run analysis, and scheduled no-action or consumer scale-up experiments. Targeted partition reassignment, the adaptive action selector, and optional hot-key splitting are research stages still to be implemented. The existing smoke test validates parts of the measurement pipeline; it does not establish the effectiveness or novelty of a mitigation policy.

The active development branch is [`main`](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/main). Earlier implementations remain available in Git history and the documented archive.

## Start here

| Purpose | Guide |
|---|---|
| Understand the project and navigate the repository | [Documentation index](docs/README.md) |
| Understand the research questions | [Research direction](docs/research-questions.md) |
| Follow the data and control paths | [Architecture](docs/architecture.md) |
| Prepare and operate the deployed environment | [Nautilus setup](docs/nautilus-measurement-update.md) and [runtime guide](docs/runtime-and-data-flow.md) |
| Interpret measurements correctly | [Metric definitions](docs/metric-definitions.md) |
| Design a comparison and assess its evidence | [Experiment methodology](docs/experiment-methodology.md) |
| Review the next proposed configuration | [Next experiment plan](experiments/next-run-review/README.md) |
| Inspect completed runs and their limitations | [Experiment records](experiment-records/README.md) |
| Read the project overview on GitHub | [Project wiki](https://github.com/sjafari2/RealTime-Streaming-Pipeline/wiki) |

## Research scope

The core study retains conventional Kafka consumer-group ownership: a partition has at most one active consumer in a group during normal operation, and the application processes that partition sequentially. Additional consumers can process different partitions in parallel. They cannot divide one overloaded partition's sequential work among multiple consumers.

The research evaluates partition-level monitoring, consumer scaling, and planned targeted ownership changes. A later decision rule will use the results to choose an appropriate action, including waiting when intervention is unlikely to help. Producer-side key splitting remains an optional study for compatible processing semantics. Its results would require separate ordering and partial-result correctness evidence.

## Repository layout

```text
src/                    Producer, consumer, shared runtime, configuration template
my-shell/               Managed run coordinator and operational entry points
python-scripts/         Offline outcome, backlog, execution and repetition analysis
tests/                  Local behavior and measurement checks
docs/                   Architecture, research, operations and metric documentation
experiments/            Proposed or reviewed experiment configurations
experiment-records/     Small run summaries, configurations and evidence hashes
dockerfiles_confluent/  Application images and runtime dependencies
k8s/ helm/ charts/      Infrastructure manifests and deployment resources
grafana/                Monitoring dashboard definitions
archive/                Superseded implementations with provenance
```

Large raw run evidence is stored separately from Git. The generated `results/` directory is ignored. Read [Git history and backups](docs/git-history-and-backups.md) for the distinction between source history, small experiment records and raw-data backups.

## Running an experiment

Complete the [deployment and synchronization steps](docs/nautilus-measurement-update.md) first. These commands operate an existing configured cluster; cloning the repository alone does not provision a working experiment environment. Review the workload, available storage and run duration before starting.

From the repository root, one complete run uses:

```bash
bash my-shell/save-run.sh
```

To repeat the current shared configuration five times:

```bash
bash my-shell/run_pipeline.sh --repetitions 5
```

The coordinator checks readiness, freezes the configuration, schedules production and drain, retains evidence, exports monitoring data and calculates summaries. Every run has a separate identifier and output directory. A failed batch stops and preserves its completed outputs. Fresh topics are created; old topics are not automatically removed.

The shared `/config/pipeline-configmap.yaml` is edited while applications are stopped. `src/pipeline-configmap.yaml` is a local template. Code synchronization does not automatically apply that template to the live configuration. See [runtime commands](docs/runtime-and-data-flow.md) for rate sweeps, interruption, export and scheduled scale-up.

## Reading results

Report completion latency together with unfinished records, actual admission and completed throughput. Pair backlog and resource-time estimates with their measurement coverage. Keep rolling Grafana attempt metrics separate from whole-run, distinct-message cohort results.

The current synthetic completion endpoint precedes commit acknowledgement and does not represent a durable external business result. A 99 ms deadline is provisional. Clock uncertainty, missing measurements and the scope of resource accounting must accompany conclusions. See [limitations and roadmap](docs/limitations-and-roadmap.md).

## Local checks

In an isolated Python environment:

```bash
python3 -m pip install -r dockerfiles_confluent/runtime-requirements.txt pytest
python3 -m pytest -q tests
```

Local checks do not replace live deployment validation or repeated performance experiments. Contributions should follow [the contribution guide](CONTRIBUTING.md) and preserve the distinction between implemented behavior, proposed experiments and observed results.
