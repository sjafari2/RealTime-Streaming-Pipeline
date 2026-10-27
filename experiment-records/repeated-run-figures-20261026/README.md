# Repeated-run figures

These figures summarize existing trials; no new experiment was run. The balanced series contains two runs at each of six aggregate input rates. The backlog series contains two runs for each of four responses with twelve high-input partitions at 700 messages/s.

## Reproduce

From the repository root, with NumPy and Matplotlib installed:

```bash
python3 experiments/result-metric-audit-20260930/build_repeated_run_figures.py \
  --data experiment-records/repeated-run-figures-20261026 \
  --output results/repeated-run-figures
```

`balanced-comparison.json` copies the reviewed balanced summary. `backlog-repeats.json` selects total-lag observations from the reviewed intervention metrics and records both original source hashes. Raw experiment evidence remains in the separately backed-up results storage.

## Interpretation

- The rate-response figure retains both runs, displays their arithmetic mean, and uses min–max whiskers. Connecting lines are visual guides, not a fitted performance model. Completion p99 is calculated separately for each run before averaging; it is not a pooled-message percentile. Growth covers the final ten evaluation minutes. P99 and unfinished fractions follow evaluation messages through drain end.
- The backlog figure aligns the nominal two-second observations by evaluation time. The solid curve is the mean of two runs and the shading is one sample standard deviation (denominator n−1). Thin curves retain individual runs. A pair is summarized only when both measurements are valid. Missing observations and explicit ownership-transition break markers remain gaps. No interpolation or zero filling is performed.
- The four backlog panels use different vertical scales to show recovery behavior. The one-minute marker is the scheduled intervention time, not activation confirmation; it is a reference boundary for Keep 3. Warm-up and drain are outside this plot.
- With only two runs, the standard deviation is descriptive. It is not a confidence interval, physical bound, or demonstration of general reproducibility. A mean-minus-SD band can extend below zero although actual backlog cannot.

The layouts adapt the general measured-point/range and mean/variation-band ideas in Figures 6 and 9 of *Denoising Application Performance Models with Noise-Resilient Priors*. All plotted data are from this Kafka pipeline; no predictive-model or injected-noise experiment is implied.

## Validation

Original input hashes matched the repository records. All 18 balanced summaries and 1,165 paired backlog mean/SD samples were independently checked with Python's statistics module. Missing-data masks were checked for all 1,200 expected two-second pairs. The generated PDFs were rendered for visual inspection, and the proposal compiled with zero errors and warnings.
