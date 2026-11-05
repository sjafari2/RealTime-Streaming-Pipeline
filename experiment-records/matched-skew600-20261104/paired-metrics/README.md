# Mean results for matched traffic concentrations

These figures use the 16 validated trials listed in ../completion.json. Each response has two runs with 12 high-traffic partitions and two with 4, under the same 600 messages/s deployment. The figure set covers the 13 metric roles in the current proposal table. Consumer capacity remains unmeasured because matching saturated calibration evidence is unavailable.

Curves and points give equal weight to the two runs. Bands and whiskers show their observed range, not uncertainty bounds. Missing values are not filled with zero or interpolated. Run p99 values are averaged without pooling message records. Recovery means require confirmation in both runs. Resource cost is the observed CPU and RSS time integral over evaluation and drain, rather than requested allocation.

The five figure groups are control signals for intervention need, partition arrivals and capacity availability, throughput and window p99, run-level outcomes, and resource and activation costs. `two-run-means.json` retains each run-level value alongside its mean. `plot-data.json.gz` retains the time-series values and evidence hashes. The analysis is implemented in experiments/paired-metric-means. Large raw records remain in separately backed-up storage.

The proposal passage is prepared for review and has not been inserted online.
