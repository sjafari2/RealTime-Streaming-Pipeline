# Four hot partitions at a calibrated balanced input rate

This prospective comparison keeps the aggregate input at **700 messages/s**, a rate that showed bounded backlog under balanced input in both earlier calibration runs. It changes the concentration of traffic: **80% goes to four of sixty partitions**, initially all on Consumer 2. This is different from the earlier 80/20 workload, which used twelve hot partitions. Stability under balanced input does not guarantee stability under skew.

Two repetitions compare redistribution within three consumers with scaling to six **with targeted redistribution**. Both use the same explicit ownership handover. Native Kafka scaling/rebalancing is not a performance condition in this block. Each performance trial has 60 seconds warm-up, 600 seconds evaluation, and 120 seconds drain. The action is scheduled 60 seconds into evaluation; actual decision, enrollment and handover times are retained.

| Condition | Hot partitions per consumer after the action | Expected aggregate input per consumer (messages/s) |
|---|---|---|
| Redistribute within 3 | 2, 1, 1 | 327.5, 187.5, 185 |
| Scale to 6 with targeted redistribution | 1, 1, 1, 1, 0, 0 | 165, 165, 162.5, 162.5, 22.5, 22.5 |

These rates follow the configured probabilities and assignment, not measured processing capacity. The existing assignment rule distributes hot and cold partition counts separately. It is not an optimal load- or capacity-weighted assignment. If six performs better, that does not establish that every possible three-consumer assignment would fail; assigning fewer cold partitions or choosing a faster owner for the double-hot load could help. Every valid unfavorable outcome is retained.

The first repetition runs redistribution then scaling with redistribution. The second reverses this order. Workload seeds are 81 and 82; both methods share the seed within a repetition. Empty-topic preparations capture and verify the starting map and original pod identities/nodes. The four hot partitions are selected from Consumer 2's initial ownership before traffic, and that set stays fixed throughout the comparison. The existing low-input native technical check is separate from the four performance trials.

The primary completion deadline is **1 second**, with **0.5 seconds as a stricter sensitivity threshold**; both are fixed before these trials. Balanced 700 messages/s calibration recorded p99 of 0.248 and 0.395 seconds. Those observations provide context but do not establish an application requirement, maximum latency or future guarantee. These are experimental targets, not validated application SLAs. Historical 99 ms results retain their original threshold.

At each threshold, the cohort miss rate is the number of distinct evaluation messages that completed late, plus those unfinished at the cutoff whose deadlines have elapsed, divided by the number of distinct acknowledged evaluation messages. A completion exactly at the deadline is on time. A message is counted once, using its earliest completion before the original cutoff. Not-yet-expired unfinished deadlines are censored; the full-cohort rate is unavailable if any remain censored or a completion clock is invalid. Observed-completion violation rates use completion attempts during evaluation and are reported separately. A latency threshold alone does not define an acceptable violation percentage; no application-level compliance claim is made.

Both results are calculated from the same evidence in one reconciliation pass and saved in `comparison.json` and the report. The secondary threshold changes reporting only, not processing or the observation cutoff. The 1-second threshold is stored in `SLO_THRESHOLD_MS`; both reporting thresholds are stored in the campaign protocol.

Primary outcomes are unfinished messages, recovery during continuing input, final-two-minute backlog growth, and whole-run completion p99. The fixed late production cohort uses evaluation seconds 480–600 and the original drain cutoff. It is called post-recovery only if recovery is confirmed before its start. The [metric coverage guide](METRIC_COVERAGE.md) specifies the additional diagnostics. CPU, memory, requested resources, transition interruption and accumulation remain included.

The first block uses 700 messages/s. Any follow-up is limited to 800 messages/s; 900 and 1,200 are not enabled by this runner. No favorable result is assumed and no higher-rate run is scheduled automatically.

Preview the configuration from the repository root:

```bash
python3 experiments/four-condition-20260929/run.py --aggregate-rate 700 --hot-partitions 4 --targeted-only --slo-ms 1000
```

After authentication, readiness and source checks:

```bash
python3 experiments/four-condition-20260929/run.py --aggregate-rate 700 --hot-partitions 4 --targeted-only --slo-ms 1000 --monitoring-gate experiment-records/monitoring-gap-20260929/live-verification.json --execute
```

The runner prints its campaign directory and restores the prior shared configuration and stopped three-consumer baseline. Raw evidence stays under `results/`. Only small reviewed summaries, plots, provenance and evidence hashes belong in Git. The committed protocol describes the intended design; it is not evidence that trials have completed.

Completed evidence is available in the [four-trial report](../../experiment-records/four-hot-partitions-20260930/README.md), with [late-period results and persistent-hotspot timing](../../experiment-records/four-hot-partitions-20260930/LATE_COHORT.md). The prospective protocol above remains the record of settings chosen before those outcomes.
