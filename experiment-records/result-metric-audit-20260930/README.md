# Results metric audit and proposal-ready material

This record covers twenty existing performance trials: eight twelve-partition ten-minute trials, four four-partition ten-minute trials, and two earlier four-trial five-minute blocks. No new experiment was run. The repository total remains sixty completed performance trials.

Main results combine the ten-minute and new four-partition plots into three figures per workload: outcomes, performance/resources, and runtime state with partition-lag diagnostics. This replaces ten separate figure pages per workload while retaining the metrics. Ownership and resource cost use compact tables. Detailed individual plots remain available alongside the combined figures. Five-minute-evaluation blocks and half-second deadline sensitivity belong in the appendix. Earlier calibration and historical background are not silently removed.

The one-second deadline is the main outcome. Half-second sensitivity is secondary. Both were prespecified for the four-partition campaign; applying them to earlier blocks is retrospective. The original 99-ms data are preserved.

## Integration

- Read the current online Measurement and Results sources before applying any replacement. These files are a reviewable insertion/replacement package, not a claim that the online proposal has been updated.
- Insert `Measurement_Coverage_Results.tex` at the beginning of the relevant results discussion, reconciling terminology with the current Measurement section.
- Replace the existing twelve-partition comparison with `Twelve_Partition_Results_Updated.tex`, avoiding a second duplicate report.
- Add `Four_Partition_Results_New.tex` immediately afterward. Keep the two workloads separate.
- Put `Measurement_Coverage_Appendix.tex` in the appendix; retain the interpretation text in main results.
- Keep `Five_Minute_Metrics_Appendix.tex` and `Deadline_Sensitivity_Appendix.tex` in the appendix. Retain the existing five-minute process plots alongside the added diagnostics.
- Copy the PDFs from `figures/` into the project's figure directory. Remove old appendix includes for the ten-minute figures so each appears once in main results.
- Compile, inspect changed pages and confirm all labels, figures and references resolve before claiming online completion.

## Evidence and limits

`metrics.json` records run identifiers, source hashes, exact statistics, gap-preserving plot data, deadline populations and coverage. `runtime-states.csv` and `runtime-states.json` retain the four runtime components at each observation, including the actual persistent-hot partition set. B and S use fifteen observations (28 seconds); G uses fifteen intervals (30 seconds). These reconstructed states are not evidence of an online controller. The separate processing-growth plot uses ten seconds. `compact-plot-data.json` retains the compact resource and outcome traces and event-file hashes. Time-series summaries cover evaluation only; process and requested-resource integrals cover evaluation plus drain. Requested costs for the first twelve-partition unchanged baseline are unavailable over the full interval because most request history is missing. Actual process integrals sum observed intervals, do not extrapolate gaps, and exclude brokers, producers and sidecars. Scaled consumers' whole-window coverage includes time before startup.

`build_report.py` verifies source summary hashes and reconciles retrospective deadline replay with original cohort counts and p99. `build_compact_figures.py` reconstructs runtime states and creates compact comparison panels. `build_documents.py` creates the accompanying LaTeX, compiled separately for PDF review. Large raw logs and local replay caches remain outside this record.

## Verification and navigation

- [Compact results report (PDF)](Results_Metric_Update.pdf)
- [Twelve-partition runtime state](figures/twelve-partition-runtime-state.pdf)
- [Four-partition runtime state](figures/four-partition-runtime-state.pdf)
- [Timestamped state values (CSV)](runtime-states.csv)
- [Metric coverage](metric-coverage.md)
- [Reproduction commands](../../experiments/result-metric-audit-20260930/README.md)

All 4,063 fully defined reconstructed states across twenty trials were independently checked against the window-mean, growth and persistent-set definitions. Six targeted tests passed. All twelve compact outcome/performance panels' event-file hashes match the published comparison records; admitted and completed counts reconcile. The standalone LaTeX report compiles without warnings and its changed figure/table pages were visually inspected. The report uses three compact figure pages per main workload instead of ten separate pages; detailed plots are retained here. This is an analysis update, not twelve or twenty additional trials.

The historical source checkout for this analysis was `d9d201ebb7bfb1443d6b5dd24a7dd4f0c28723dd`. This reporting revision does not alter the code revisions recorded for the original trials. The main results discuss one-second completion deadlines, with half-second sensitivity in the appendix. Raw skew remains in the detailed diagnostic figures and numeric summaries; the runtime-state panel shows window-mean skew.

## Compact experiment overview

`Starting_Configuration.tex` and `Experiment_Plan.tex` provide the configuration and concise completed/planned experiment tables. Include configuration first and load `tabularx` and `colortbl` for the table layout and section colors. `Experiment_Tables.pdf` previews both tables and the measurement-coverage appendix. The overview covers 36 trials presented in the proposal, including 12 balanced-rate calibration trials; it does not change the twenty-trial metric-audit scope or the repository total of sixty trials.

The primary comparison deadline is one second. The main skewed trials use one minute of warm-up, ten minutes of evaluation and two minutes of drain (13 minutes total). Balanced calibration uses twenty minutes of evaluation with the same warm-up and drain periods (23 minutes total); earlier five-minute evaluations total eight minutes. During drain, producers stop and consumers continue processing until the fixed cutoff.
