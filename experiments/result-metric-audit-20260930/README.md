# Reconstructing comparison metrics and runtime states

These scripts analyze retained trials without changing runtime configuration or rerunning traffic. The versioned results are in `experiment-records/result-metric-audit-20260930/`.

From the repository root, with NumPy and Matplotlib installed:

```bash
python3 experiments/result-metric-audit-20260930/build_report.py --root . --output /tmp/pipeline-result-audit
python3 experiments/result-metric-audit-20260930/build_compact_figures.py --root . --output /tmp/pipeline-result-audit
python3 experiments/result-metric-audit-20260930/build_documents.py --root . --output /tmp/pipeline-result-audit
```

The raw `results/` evidence is required. Optional `--outcome-cache` for the first command can reuse reconciled retrospective deadline analyses. Temporary per-message replay caches and event plot caches remain outside Git. Compile `Results_Metric_Update.tex` in the output directory with a LaTeX engine after all figures are generated.

`build_report.py` verifies original summary hashes and reconciles outcome counts and p99 before adding 0.5-second and one-second deadlines. `build_compact_figures.py` verifies event hashes against the published comparison, reconstructs the original windowed state, and aggregates resource samples only when all active consumer processes have fresh measurements. Missing observations and ownership discontinuities stay unavailable. The observed resource integrals sum covered intervals; they do not extrapolate unknown values.

The first four rows of each runtime figure correspond to windowed backlog, growth, skew and the persistent-hot set size. Supporting CSV and JSON preserve the full set. Fifteen backlog/skew/persistence observations span 28 seconds on the two-second grid; fifteen growth intervals span 30 seconds. The separate processing-backlog growth figure uses ten seconds. These offline states do not imply that an adaptive controller selected the scheduled interventions.

The local verification covers missing samples, unequal sample intervals, offset resets, ownership transitions, set-versus-count handling, and resource aggregation. Run `python3 -m pytest -q tests/test_result_metric_audit.py`. Original runtime revisions and evidence hashes remain associated with the original trials.
