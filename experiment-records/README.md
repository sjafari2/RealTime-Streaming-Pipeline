# Versioned experiment evidence

The [experiment register](../EXPERIMENT_REGISTER.md) summarizes progress in plain language. The completed inventory contains **60 performance trials**; the [current-status table](../docs/current-status.md) separates the experimental blocks and their evidence.

The [four-condition comparison](four-condition-20260929/README.md) contains two thirteen-minute trials per condition, whole-run outcomes, lag and resource plots, observed ownership, and measured intervention costs. The [monitoring correction](monitoring-gap-20260929/README.md) records the diagnosis and technical validation that preceded those trials.

The latest [four-high-input-partition comparison](four-hot-partitions-20260930/README.md) adds four thirteen-minute trials at 700 aggregate messages/s. Both repetitions retained the unfavorable six-consumer latency outcome. Whole-run outcomes, fixed late-period cohorts, hotspot timing and deadline sensitivity remain distinct.

This directory contains compact summaries, reviewed configurations and evidence hashes. Full per-message evidence and monitoring exports remain in the ignored `results/` directory and on separately managed storage. A versioned summary is not a backup of its raw evidence.

Each reviewed block records its run identifiers, workload and timing, actual configuration, measurement coverage, validity checks and known execution revision. Prepared configurations and technical checks are distinguished from performance trials. Historical reports keep their original scope: the [12 September report](campaign-20260912/campaign-report.md) covers sixteen trials, and the [original per-run table](paired-table-20260914/paired-results-data.json) covers the later twenty-four-trial inventory. Neither is the full current inventory.

Original runtime hashes and package versions preserve source identity. Later analysis and documentation revisions do not replace the revision used to execute a historical run. The run `run-20260911-214924` predates Git integration; its runtime evidence is retained, but its historical Git revision is explicitly unknown.

Corrections are committed as new records or revisions, preserving the earlier interpretation in Git history. Missing observations remain unavailable, valid unfavorable outcomes are retained, and failed preparations have separate records.
