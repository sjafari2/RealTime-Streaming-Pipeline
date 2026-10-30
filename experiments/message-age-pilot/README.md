# Queue-age pilot

This is an isolated test of whether the elapsed age of waiting records improves a latency diagnostic attached to a reconstructed assignment rule. It uses real Kafka messages and the pipeline's sequential 2,000-iteration SHA-256 task. It is separate from the managed performance campaigns and does not implement a production controller.

The baseline follows the published action branches of Ezzeddine et al. (2025), with the packing assumptions recorded in the accompanying decision-rule audit. Its output determines the baseline assignment; it is not forced to use six consumers. A second response uses a targeted six-consumer assignment. All six pods are prepared before measurement, so the probe does not measure scale-out, reassignment interruption, or resource-cost savings.

Each episode creates a fresh topic with 60 partitions. A preload places 300 records in each of twelve high-input partitions. While application processing remains gated, an identical five-second trace produces another 3,000 records at 600 messages/s, with 80% sent to those twelve partitions. The resulting initial cohort contains exactly 6,600 acknowledged records. The two preparation conditions add either zero or eight seconds of holding before the common trace. Actual creation timestamps and ages are retained; startup and delivery delay can make the realized ages differ from the nominal added holding time.

After release, the same input continues for 20 seconds. The observation ends 30 seconds after release. Each response and preparation condition is repeated twice, with the order reversed in the second repetition. The 20-second diagnostic deadline concerns messages already waiting at release; 10 and 15 seconds are secondary checks. This is not a change to the proposal's SLO or run schedule.

Two numerical latency diagnostics are saved before processing is released. Both use the same pooled processing rate, final assignment, and oldest-first service assumption. One ignores elapsed message age; the other includes it. These numerical p99 estimates are not outputs of the published decision rule itself. Actual processing follows the order returned by Kafka `consume()`, and processing starts and completions are retained for checking the service assumption.

A preliminary task-rate diagnostic runs on each consumer before the comparison. It measures the task-processing rate, not full pipeline capacity. The rule uses the average over the original three consumers, following the paper. Individual rates remain available to identify heterogeneity rather than hiding it behind that average.

Run from the repository root:

```bash
python3 experiments/message-age-pilot/run_probe.py \
  --output /path/to/separate-raw-evidence/age-pilot --execute

python3 experiments/message-age-pilot/analyze_age_probe.py \
  /path/to/separate-raw-evidence/age-pilot \
  --output /path/to/age-pilot-summary.json

python3 experiments/message-age-pilot/plot_age_probe.py \
  /path/to/age-pilot-summary.json /path/to/age-pilot.png
```

The runner requires three stopped managed consumers and checks the shared run-control state before proceeding. It prepares six replicas temporarily, checks clocks over established connections, writes only a new `/config/age-probe-*` directory, and restores the original replica count in its cleanup path. Synthetic topics use one-day retention. Existing topics and experiment records are not deleted.

The raw archive is retained locally and on the cluster. Independent analysis checks producer acknowledgments, distinct completion identifiers, per-partition offset continuity, sequential processing, assignment ownership, matching queue counts, prediction timing, and original pod identities. Completion p99 is reported with unfinished counts. The fraction completing within the deadline uses the entire initial cohort, including unfinished records.

The results cannot by themselves establish an age-only improvement, because the pooled capacity model, per-consumer rates, Kafka delivery order and preparation ages also affect the predictions. A successful diagnostic case is not evidence of a validated controller or novel contribution.
