# Experiment methodology

Calibration selects a balanced input rate that three consumers can sustain over the observation period before comparing mitigation responses. The original deployment used **700 aggregate messages/s** after two balanced trials at each of 600, 700, 800, 900, 1,200 and 1,500 messages/s. After resource and placement changes, two 700 messages/s checks were stopped early because processing backlog continued to grow. A separate full twenty-minute check supported **600 messages/s** for the revised deployment: achieved input was 600.00 messages/s, completion throughput was 599.99 messages/s, processing backlog ended at 79 offsets, and all 720,000 evaluation messages completed by drain end. This is one completed calibration run, not a general capacity guarantee. Placement also changed, so the checks do not isolate input rate alone.

Each deployment's selected rate remains fixed across its mitigation comparisons. Producer acknowledgment records verify achieved rates in every trial; this check does not repeat capacity calibration. The additional calibration attempts are separate from the 32 reported trials.

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

Actions are scheduled 60 seconds after evaluation begins. Run 1 and Run 2 are separate trials for each condition, with condition order reversed in the second comparison and a common workload seed within each comparison. The twelve-partition block tests keeping three consumers, redistribution within three, targeted scaling to six and native Kafka scaling to six. The four-partition block at 700 messages/s compares the two targeted responses; the revised 600 messages/s block compares all four responses.

Native scaling uses Kafka's cooperative-sticky group assignment. Targeted actions use explicit release, acquire and resume stages with exclusive ownership and verified offsets. The comparison therefore includes coordination behavior and resulting assignment, as well as resource count. A whole partition remains indivisible.

## Reproducible producer streams

The revised producer derives its partition-choice sequence from `WORKLOAD_SEED` and a separate `PRODUCER_WORKLOAD_ID`. The deployment supplies IDs `0`, `1`, and `2` from the StatefulSet ordinal label, so renaming the StatefulSet does not change those streams. The coordinator rejects missing or repeated IDs before releasing traffic and saves the verified IDs, seed and RNG scheme in the run manifest. Each producer also records them in its configuration event and status.

With the same seed, ID, workload settings and compatible Python runtime, each producer repeats its sequence of partition choices. This does not guarantee identical send timestamps, inter-producer arrival order or achieved rates. Actual pod names remain in message identities and monitoring. The original trials seeded streams with the pod hostname; switching to ordinal IDs changes their sequences and must be recorded as a new workload-generation revision. It does not change or invalidate the original measurements.

## Placement in subsequent trials

The revised deployment requires separate nodes for producers and consumers in the experiment namespace, including added consumers. The coordinator checks the deployed rules and actual placement before production and after scaling, and rejects unmet requirements. Broker isolation is a separate explicit option. These controls do not exclude unrelated workloads or guarantee equal machine capacity. The original trials used softer scheduling preferences and include producer–consumer co-location; their measurements remain unchanged. New trials must record the revised placement conditions and repeat calibration and starting checks. See [node placement](node-placement.md) for the evidence and exact scope.

## Measurement and interpretation

Producer acknowledgments are reconciled with distinct message identities and earliest completion records. Completion follows application processing and precedes commit acknowledgment. Whole-cohort p99 includes completed evaluation messages; unfinished messages are reported separately against all acknowledged evaluation messages. It is not an average of rolling dashboard p99 values.

Lag, processing backlog, growth, skew, CPU and resident memory retain measurement coverage. Requested consumer resources are integrated over evaluation plus drain. A missing interval is unavailable, not zero. The primary reported experimental completion deadline is one second; the half-second threshold is a sensitivity check. These thresholds were specified before the four-partition block and applied retrospectively to the twelve-partition block. Original measurements retain their original settings.

Rejected preparations are documented separately from performance trials. All valid outcomes, including unfavorable ones, remain in their comparison. Two trials per condition provide preliminary evidence, not a general ranking or a small-sample confidence claim. Exact formulas are in the [metric reference](metric-definitions.md); configurations and execution revisions are linked from the [results](../experiment-records/README.md).

## Changes to the resource budget

The [October 20 deployment check](../experiment-records/resource-sizing-20261020/README.md) and [October 21 recalibration](../experiment-records/resource-calibration-20261021/README.md) record the resources and placement behind the revised reference rate. Increasing consumer allocations from 1.25 CPUs / 640 MiB to 2 CPUs / 768 MiB did not remove growing backlog in the revised 700 messages/s checks. Further deployment changes require renewed calibration and checks of achieved input, processing capacity, memory headroom and CPU throttling. Each comparison holds accepted resource declarations fixed and reports actual use separately from requested-resource cost. Results remain associated with their recorded deployments.
