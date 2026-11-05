# Paired metric comparisons

The figures compare the two independent repetitions of each response using equal run weights. The smaller and larger starting-backlog conditions remain separate. Bands and whiskers show the observed run range, not uncertainty intervals. A time-series mean requires both repetitions to have a valid observation, and unavailable intervals remain gaps.

The five figure groups cover the thirteen metric roles in the proposal's metric table. Total backlog, backlog growth, lag skew and message age describe intervention need. Partition arrival rates and calibrated consumer capacities describe demand and candidate feasibility. Performance includes throughput, p99 latency, unfinished messages, SLO violations and recovery. Costs include observed consumer CPU and memory integrals and decision-to-active time.

## Definitions

- Backlog and skew use the saved 15-observation rolling means. Backlog growth uses 15 consecutive changes in total consumer-position lag. It is not processing-backlog growth.
- Message age is the arithmetic mean time since creation among acknowledged messages still awaiting completion at each two-second observation, including warm-up work. An empty population has no age value.
- Throughput counts distinct completions in consecutive 30-second intervals, including warm-up messages completed during evaluation. Window p99 uses the full latencies of evaluation messages completed in that interval.
- Whole-run p99, unfinished percentage and SLO violations follow the evaluation-production cohort through the drain cutoff. The plotted mean of two run-level p99 values is not a percentile of pooled messages.
- Recovery retains the executed criterion of processing backlog no greater than 100 offsets for 30 continuous valid seconds during continuing input. Recovery time is measured from the common scheduled decision. A missing recovery time is not zero or the observation cutoff. No two-run recovery mean is reported when either repetition is unconfirmed.
- Partition rates use fresh producer acknowledgment counters over the pre-action interval. Counter availability is assessed independently of lag-window validity. Consumer capacity is shown only where matching busy calibration established it, rather than inferred from underloaded throughput.
- Resource costs integrate observed consumer CPU in core-minutes and RSS memory in GiB-minutes from evaluation start through drain. Coverage is relative to recorded process operating time, excluding the time before an added consumer started. Missing intervals are omitted, not filled with zero.

The gray dotted line marks the action request at evaluation minute 1. Colored dotted lines mark each repetition's confirmed activation. The dashed throughput line is the continuing input of 600 messages/s.

## Starting-backlog findings

All intervention runs completed the evaluation cohort by drain end. Keep 3 left means of 31.0% and 38.9% unfinished for the smaller and larger conditions. Compared with redistribution, targeted scaling reduced mean recovery from 239 to 174 seconds and from 426 to 247 seconds, respectively. Its corresponding mean run-level p99 increased from 54.0 to 80.5 seconds and from 108.1 to 152.6 seconds, and its consumer resource costs were higher.

Kafka scaling had the lowest mean run-level p99 in both conditions. Recovery was confirmed in only one smaller-backlog repetition, while both larger-backlog repetitions recovered. The smaller-backlog run without confirmed recovery also had a more concentrated final partition distribution. The comparison does not isolate backlog size from message age or resulting ownership.

## Reproduction

Run `experiments/paired-metric-means/plot_metrics.py` with `--comparison` pointing to the retained starting-backlog `comparison.json` and `--output` pointing to a local results directory. Raw evidence must be restored at the recorded run paths. The script verifies evidence hashes, reconciles cohort counts and p99, and checks that interval throughput integrates to the recorded evaluation throughput.

`analyze_matched.py` follows the matched sixteen-trial 12-versus-4 campaign, reconciling completed trials as they become available. It publishes pair means only after all planned trials pass validation and deployment restoration is verified. Matching capacity estimates are not assumed from a balanced sustainability check.

The compressed plot data, two-run numerical summaries and validation record preserve small reproducible analysis artifacts. They do not replace the separately retained raw message and monitoring evidence.
