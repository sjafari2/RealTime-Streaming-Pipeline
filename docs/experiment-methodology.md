# Experiment methodology

I use calibration to distinguish a sustainable balanced workload from overload before comparing mitigation responses. The current reference rate is **700 aggregate messages/s** across three producers. It was selected after two balanced trials at each of 600, 700, 800, 900, 1,200 and 1,500 messages/s. Bounded backlog over a finite observation period supports the chosen reference; it does not establish permanent stability or a universal capacity limit.

## Configuration and phases

Each trial uses a fresh topic, a saved run configuration, 60 partitions and a synthetic processing task of 2,000 SHA-256 iterations per record, with no added sleep. Shell scripts launch experiments with configurable parameters; Python coordinates execution and collects results. Producer and consumer processes load the same saved configuration and share phase boundaries.

| Published block | Warm-up | Evaluation | Drain | Total per trial |
|---|---:|---:|---:|---:|
| Balanced-rate calibration | 1 min | 20 min | 2 min | 23 min |
| Twelve-hot-partition comparison | 1 min | 10 min | 2 min | 13 min |
| Four-hot-partition comparison | 1 min | 10 min | 2 min | 13 min |

Warm-up allows initialization and processing before the evaluation cohort begins. Warm-up messages are excluded from cohort latency and unfinished percentage, but remaining warm-up work stays queued and can delay evaluation messages. After production stops, consumers continue until the fixed drain cutoff.

## Controlled comparisons

The skew trials send 80% of input to either twelve or four partitions initially owned by one consumer. Empty-topic preparations capture and verify starting ownership, original pod identities, nodes and resource declarations before traffic is released. This reduces starting differences that could otherwise favor an intervention. It does not remove variability on shared machines.

Actions are scheduled 60 seconds after evaluation begins. Run 1 and Run 2 are separate trials for each condition, with condition order reversed in the second comparison and a common workload seed within each comparison. The twelve-partition block tests keeping three consumers, redistribution within three, targeted scaling to six and native Kafka scaling to six. The four-partition block compares the two targeted responses only.

Native scaling uses Kafka's cooperative-sticky group assignment. Targeted actions use explicit release, acquire and resume stages with exclusive ownership and verified offsets. The comparison therefore includes coordination behavior and resulting assignment, as well as resource count. A whole partition remains indivisible.

## Measurement and interpretation

Producer acknowledgments are reconciled with distinct message identities and earliest completion records. Completion follows application processing and precedes commit acknowledgment. Whole-cohort p99 includes completed evaluation messages; unfinished messages are reported separately against all acknowledged evaluation messages. It is not an average of rolling dashboard p99 values.

Lag, processing backlog, growth, skew, CPU and resident memory retain measurement coverage. Requested consumer resources are integrated over evaluation plus drain. A missing interval is unavailable, not zero. The primary reported experimental completion deadline is one second; the half-second threshold is a sensitivity check. These thresholds were specified before the four-partition block and applied retrospectively to the twelve-partition block. Original measurements retain their original settings.

Rejected preparations are documented separately from performance trials. All valid outcomes, including unfavorable ones, remain in their comparison. Two trials per condition provide preliminary evidence, not a general ranking or a small-sample confidence claim. Exact formulas are in the [metric reference](metric-definitions.md); configurations and execution revisions are linked from the [results](../experiment-records/README.md).
