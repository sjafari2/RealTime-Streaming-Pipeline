# Small experiment records kept in Git

**Start here:** [Experiment register and progress](../EXPERIMENT_REGISTER.md). Update it after each reviewed run or bounded block; keep exclusions and approval status visible.

This directory keeps reviewable summaries and frozen configurations. Raw per-message
evidence and full monitoring exports remain in the ignored `results/` directory and
on separately managed storage. These records are not a replacement for a data backup.

The [12 September 2026 campaign report](campaign-20260912/campaign-report.md) links
all 16 controlled trials, paired plots, calibrations, technical exclusions and
operational completion status. It also identifies which proposal conditions were
tested and which remain for the main study.

For each completed run, retain its run ID, workload/timing, actual configuration,
summary metrics, validity/coverage limits, evidence checksums and known source revision.
Never label illustrative values or a prepared configuration as an executed run.
Capture the Git revision before a new run; do not assign the current revision to
older experiments retrospectively. The archived runtime events also retain application
source hashes and package versions.

Commit a completed record with a descriptive message after checking it. Correct a
record with a later explanatory commit so the earlier interpretation remains visible.
The run `run-20260911-214924` predates this Git integration; its source identity is
retained in its evidence and deployment audit, and its historical Git revision is
explicitly unknown.

The [controlled repeat](static-startup-executed-20260912/README.md) adds four successful empty preparations and two completed performance trials with a common frozen starting reference.

The [80/20 comparison](8020-comparison-20260913/README.md) adds two trials with 80% directed to 12 of 60 partitions, following four successful empty rehearsals.
