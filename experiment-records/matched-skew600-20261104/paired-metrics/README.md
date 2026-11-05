# Mean results for matched traffic concentrations

These figures use the 16 validated trials listed in ../completion.json. Each response has two runs with 12 high-traffic partitions and two with 4, under the same 600 messages/s deployment. The figure set covers the 13 metric roles in the current proposal table. The capacity panels now show retrospective effective rates from eligible busy intervals, separately for each workload. These are distinct from independent calibration. Consumers without qualifying intervals remain unestimated, as detailed in `../capacity-audit/`.

Performance and cost curves and points give equal weight to the two runs. Capacity points instead divide completed attempts by observed seconds across eligible intervals, with whiskers showing the observed interval-rate range. These intervals are not independent repetitions. Bands and whiskers show their observed range, not uncertainty bounds. Missing values are not filled with zero or interpolated. Run p99 values are averaged without pooling message records. Recovery means require confirmation in both runs. Resource cost is the observed CPU and RSS time integral over evaluation and drain, rather than requested allocation.

The five figure groups are control signals for intervention need, partition arrivals and capacity availability, throughput and window p99, run-level outcomes, and resource and activation costs. `two-run-means.json` retains each run-level value alongside its mean. `plot-data.json.gz` retains the time-series values and evidence hashes. The analysis is implemented in experiments/paired-metric-means. Large raw records remain in separately backed-up storage.

The proposal passage is prepared for review and has not been inserted online.

To reproduce the capacity audit from the repository root, set `CAMPAIGN_ROOT` to the separately stored campaign directory and `AUDIT_OUTPUT` to a working output directory, then run:

```bash
python3 experiments/paired-metric-means/audit_capacity.py \
  --repository "$PWD" --campaign-root "$CAMPAIGN_ROOT" --output "$AUDIT_OUTPUT"
python3 experiments/paired-metric-means/plot_metrics.py \
  --dataset matched-skew --comparison "$CAMPAIGN_ROOT/paired-analysis/comparison.json" \
  --output "$AUDIT_OUTPUT/figures" --capacity-audit "$AUDIT_OUTPUT/capacity-audit.json"
```

The second command regenerates the plots from the source evidence. The capacity audit is optional for the plotting script, so it must be supplied to reproduce these capacity panels. The comparison hash must match the one recorded in the audit.
