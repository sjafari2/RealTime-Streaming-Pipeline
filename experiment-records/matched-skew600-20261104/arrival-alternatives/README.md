# Alternative arrival-rate plots

These review figures use the validated matched 600 messages/s campaign. They have not replaced the existing figures or been inserted into Overleaf.

`partition-arrival-bars.png` shows the mean partition arrival rate over the two Keep 3 runs for each traffic concentration. The rates use the original pre-action producer acknowledgment-counter measurements at nominal evaluation seconds 30–60, with actual scrape elapsed time for each producer. Orange bars identify the configured high-traffic partitions. The horizontal axis is partition index, not elapsed time.

`consumer-arrival-ownership.png` compares each consumer's incoming load under the initial and final observed ownership maps for all four responses. Both maps use the same measured pre-action partition rates within each run. Each consumer's rate is the sum over its assigned partitions, then the two run-level rates receive equal weight. Whiskers show the two-run range, not confidence limits. Absent consumers are inactive, not measured as having zero capacity.

The final-map bars describe incoming demand implied by ownership. They are not measurements of traffic after the action and do not include already stored backlog. Means can conceal different Kafka assignments across repetitions, so the individual values and maps are retained in `arrival-comparison.json`. The wider whiskers in the four-partition Kafka-scaling comparison reflect those different assignments.

All 32 initial/final run-level sums were checked to equal the corresponding measured aggregate partition rate. Each of the eight comparison groups contains exactly two runs. Input hashes and calculation details are saved with the data.

From the repository root, use a Python environment with NumPy and Matplotlib:

```bash
python3 experiments/paired-metric-means/plot_arrival_alternatives.py \
  --comparison "$CAMPAIGN_ROOT/paired-analysis/comparison.json" \
  --plot-data experiment-records/matched-skew600-20261104/paired-metrics/plot-data.json.gz \
  --output "$ARRIVAL_PLOT_OUTPUT"
```

`CAMPAIGN_ROOT` points to the separately backed-up campaign evidence. `ARRIVAL_PLOT_OUTPUT` is a working output directory. Editable SVG copies accompany the PNG previews.
