# Relating signals to intervention outcomes

This offline analysis joins observations before action to the outcomes of twenty existing trials. The twelve-partition 700 messages/s, four-partition 700 messages/s and four-partition 600 messages/s blocks remain separate. It does not start experiments or train a controller.

[Results, definitions and figures](../../experiment-records/signal-outcome-20261021/README.md) include a PDF report and one CSV row per trial. Missing observations and incomplete resource histories remain unavailable.

Run from the repository root with Python 3 and the retained raw evidence:

```bash
python3 experiments/signal-outcome-20261021/build_analysis.py --output /path/to/analysis
python3 experiments/signal-outcome-20261021/build_report.py /path/to/analysis/analysis.json --output /path/to/analysis
python3 -m pytest -q tests/test_signal_outcome_analysis.py
```

The analysis uses the standard library and the existing recovery evaluator. Figure and PDF generation additionally require Matplotlib, Pillow and ReportLab; tests require pytest. `--inputs` accepts an alternative root with the same summary and historical raw-evidence layout, and `--raw600` accepts an alternative root for the eight additional runs. Original file hashes are checked before use.

Pre-action means use fifteen observations at evaluation seconds 30 through 58. Growth uses fifteen intervals from seconds 28 through 58. Cohort p99, unfinished work and deadline outcomes retain the original observation cutoff. Completion-window throughput and latency remain separate fields. The JSON retains configurations, campaign provenance, source hashes, per-consumer process resources and missing-data coverage.
