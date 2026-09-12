# Contributing to the research pipeline

Changes should keep the implementation, experiment protocol and reported evidence consistent. Use the active research branch and read the existing runtime and metric documentation before changing behavior.

## Prepare a coherent change

Describe the concrete problem and the resulting behavior. Keep unrelated edits separate. Preserve existing source history and archived implementations; do not rewrite published commits to make progress appear different from what occurred.

For a measurement change, state the event, cohort, denominator, observation interval and handling of missing or unfinished data. Update [Metric definitions](docs/metric-definitions.md), the affected analysis and relevant proposal passages together. A dashboard label must not imply a stronger endpoint than the evidence supports.

For an experiment configuration, identify the research question, treatment, baseline, controlled settings, proposed duration, resource use and expected storage. Mark configurations as proposed, reviewed or executed according to their actual status. Keep a completed run's frozen configuration unchanged.

## Validate and record

Run the checks appropriate to the change and record what they establish. Use the [local test command](README.md#local-checks) for behavioral changes. Inspect documentation links and examples for documentation-only changes. Cluster validation is separate from local tests and requires an explicitly selected experiment.

Commit completed changes with descriptive messages and push the current branch. Stage specific files; do not include unrelated work. Follow [Git history and backups](docs/git-history-and-backups.md) for provenance and backup procedures.

## Keep evidence manageable

Version source, small reviewed configurations, summaries and evidence hashes. Keep raw multi-gigabyte run data and analysis databases in separately backed-up storage. Do not add credentials, local authentication files or private cluster access material. A Git summary is not a backup of the raw experiment.

Report results with their limitations. Distinguish a successful pipeline run from a policy comparison, a reproduced paper from an adapted method, and a planned feature from implemented behavior.
