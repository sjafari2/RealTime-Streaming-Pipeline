# Saved figures for run-20260911-234758

These figures describe the single balanced Nautilus monitoring validation with three
producers, six consumers, 60 partitions and no mitigation. Production lasted five
minutes, followed by one minute of drain. They are actual measurements, with the
clock and coverage limitations recorded in the run report.

- `overview.png` / `.svg`: six-panel overview.
- `throughput`, `rolling-latency`, `deadline-misses`, `valid-lag`, `consumer-cpu`,
  `consumer-memory`: individual figures in PNG and SVG formats.
- `partition-message-counts`: exact admitted counts by partition, added after the
  whole-run evaluation finishes.
- `grafana-dashboard.png`, `grafana-latency.png`: screenshots of the existing Grafana
  dashboard with the absolute range 2026-09-11 23:49:05–23:55:20 MDT.

PNG is convenient for viewing and drafts. SVG is scalable for thesis layout. Keep
the CSV/JSON files and `queries.json` with the figures so the figures can be redrawn.
`replot.py` redraws them offline with Python and Matplotlib; `plot-environment.json`
records the versions used. It does not contact Nautilus or start an experiment.

The generated plots use PromQL filtered to this exact run ID at two-second query
steps. Throughput and deadline percentages are rolling 30-second attempt metrics;
latency percentiles are rolling histogram estimates. They are not the exact
whole-run cohort mean/p99 reported by the outcome evaluator. A rate curve can extend
past the producer stop because its preceding 30-second window still contains work.
CPU is process CPU, where 100% means one core; resident memory is in MiB.

The valid-lag plot uses the offline evaluator's complete, fresh partition snapshots
over the 300-second evaluation interval. Missing/invalid observations appear as
gaps, not zeros. Integration coverage is 94%. Other panels include the drain and
final scrape hold; the shaded region marks the 60-second drain.

The Grafana screenshots preserve the older live dashboard for visual reference.
That dashboard has older formulas, no run-ID selector and some empty panels. Its
fixed time range is useful context, but its aggregate lag/skew/STD panels should not
replace the validated offline metrics. The generated figures and their saved queries
provide the run-filtered, reproducible plot set. No dashboard definition was edited.

One run is not enough to estimate variability across repetitions or demonstrate a
mitigation effect. Cross-node clock calibration is still needed before making a
strict 99 ms deadline claim.
