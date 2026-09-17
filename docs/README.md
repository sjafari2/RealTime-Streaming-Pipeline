# Research software documentation

I maintain these guides for researchers who want to understand the experimental design, deploy the pipeline, reproduce the analysis, or extend the implementation. They describe the software in this repository and identify which results are supported by completed experiments.

[Current status](current-status.md): 24 completed performance trials, storage recovery and the six pending stability trials.

## Research design

- [Research questions](research-questions.md): the five proposal topics and their current implementation status.
- [Architecture](architecture.md): message processing, run coordination, monitoring and analysis.
- [Experiment methodology](experiment-methodology.md): a fair comparison, initial pilots and evidence requirements.
- [Limitations and roadmap](limitations-and-roadmap.md): what is established, what remains provisional and the next research stages.

## Deployment, execution, and analysis

- [Nautilus measurement update](nautilus-measurement-update.md): initial deployment and synchronization prerequisites.
- [Runtime and data flow](runtime-and-data-flow.md): supported commands, timing, file relationships and troubleshooting.
- [Metric definitions](metric-definitions.md): cohorts, completion, lag, percentiles, coverage, resource accounting and recovery.
- [Next stability experiments](../experiments/stability-20260914/README.md): six prepared configurations; execution and calibration remain pending.
- [Data and reproducibility](data-and-reproducibility.md): published evidence, raw-data availability, file formats, and analysis entry points.
- [Experiment records](../experiment-records/README.md): small records of completed runs, with explicit limitations.
- [12 September preliminary campaign](../experiment-records/campaign-20260912/campaign-report.md): all 16 controlled trials, paired plots, technical exclusions and the connection to the proposal experiment table.
- [Git history and backups](git-history-and-backups.md): source history, commits and separate raw-data preservation.

## Repository organization

The active runtime remains in `src/`; the coordinator is in `my-shell/`; offline analysis is in `python-scripts/`. The directory names are retained because deployment and synchronization use these paths. `experiments/` contains configuration plans; `experiment-records/` contains evidence about completed runs. These have different purposes.

Infrastructure resources are retained in `k8s/`, `helm/` and `charts/`. They are configuration material to inspect for the intended deployment, not a promise that every historical manifest is a current one-command installation. Superseded code is explained in [the managed-run archive](../archive/legacy-before-managed-runs/README.md) and [the earlier repository archive](../archive/repository-before-managed-runs/ARCHIVE.md).

The [GitHub wiki](https://github.com/sjafari2/RealTime-Streaming-Pipeline/wiki) is a reading-oriented entry point. Versioned source documentation is authoritative for the branch being used. Metric definitions document the implementation used to interpret the results.
