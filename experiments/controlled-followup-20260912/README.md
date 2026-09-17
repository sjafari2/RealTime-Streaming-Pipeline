# Controlled follow-up: feasibility and proposed configuration

Prepared on 12 September 2026. This is a design for review, not a deployed configuration or a completed experiment. The numerical results from the earlier campaign are unchanged.

## Decision for the available preparation time

Prioritize a controlled repeat of the existing concentrated-input comparison, keeping three consumers versus scheduling six. Targeted whole-partition reassignment is **not implemented or validated** in the active pipeline. Developing a new assignor now would add a separate engineering and correctness project. The smaller missing piece for a repeat is a startup check that rejects a different initial ownership map or machine placement before producers are released.

The saved concentrated trials identify the exact weakness: in both baselines, partition 0 started on `consumer-sts-1` on `k8s-gpu-01.calit2.optiputer.net`; in both scale trials, it started on `consumer-sts-0` on `patternlab.calit2.optiputer.net`. Consumers 0–2 had the same recorded node placement across these four runs. Opposite treatment orders therefore did not remove the different starting owner of the hot partition. Source manifest hashes and placements are in `review-plan.json`.

A matched starting map removes that particular pre-action difference. It does not make the shared machines identical, eliminate time-varying contention, or separate the hardware of newly added consumers from the scaling treatment. A new controlled result must remain a separate comparison; it cannot retroactively repair the earlier runs.

## What the code supports

| Capability | Current evidence | Status |
|---|---|---|
| Scheduled keep-three and three-to-six runs | `my-shell/run_experiment.py`, `validate_intervention` and `apply_intervention` | Implemented; used in the completed campaign |
| Target a particular partition to an existing consumer | Runner rejects actions other than `none` and `scale` | Not implemented |
| Ordinary coordinated ownership changes | `src/consumer/consumer.py` subscribes with classic `cooperative-sticky` | Implemented; not a user-selected transfer API |
| Safe offset progress for normal rebalances | Sequential completion, manual offset store, synchronous revoke commit, lost-owner handling | Existing behavior; not validation of a new transfer mechanism or exactly-once effects |
| Explicit busy partitions | Producer accepts `SKEW_PARTITION: '0,3'` | Supported; checked locally without starting a client |
| Matching initial assignment and placement | Manifest records ownership and nodes; readiness checks complete/stable ownership | Recording exists; equality against a reference and pod identity is not enforced |
| Deterministic consumer placement | StatefulSet uses a regional constraint, preferred anti-affinity and soft spreading | Does not pin a particular consumer ordinal to a node |

The dependency pin is `confluent-kafka==2.15.1`. The consumer hard-codes `group.protocol=classic` and rejects a strategy other than `cooperative-sticky`. No live broker/package inspection was performed for this review.

Confluent's cooperative callback API expects the partitions supplied by the group callback. Its built-in assignment strategies do not expose a desired partition-to-consumer map. Editing that callback is not evidence of a coordinated transfer. [Python API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.incremental_assign), [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

Kafka's Java `ConsumerPartitionAssignor` is a documented route for custom assignment. Using it would require a separate implementation, compatible membership/rebalance behavior, and matched baselines using the same client and application implementation. An eager first implementation may revoke many partitions; that cost must be reported. Switching to independent manual assignment disables ordinary group coordination and would change the experiment's semantics. [Assignor interface](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.html), [Kafka consumer](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).

## Priority comparison: repeat concentrated input with matching starts

- Three producers, 60 partitions, 1,500 target messages/s total: `TARGET_RATE='500'` per producer.
- `TRAFFIC_MODE='skew'`, `SKEW_PARTITION='0'`, `SKEW_FRACTION='0.8'`; same 100-byte payload and 2,000 SHA-256 iterations, no added sleep.
- Production 300 s, including 60 s of warm-up; bounded drain 120 s. Schedule either no action or scaling at production +120 s, which is evaluation +60 s.
- Keep the same frozen configuration, workload seed, initial three consumers, complete partition-to-consumer map and original consumer-to-node placement within a block. Use fresh run identities and the existing managed topic/reset workflow.
- Smallest comparison: one randomized two-treatment block, reported as a single descriptive comparison. If time permits, add a second seed/block with reversed order. Do not pool these as interchangeable repetitions of the old confounded campaign.

The proposed reference map assigns partition `p` to consumer ordinal `p % 3`. It was observed in prior runs, but is not guaranteed by the current dynamic-membership setup. The historical node map is a reference to recheck, not a claim about today's cluster. Capture current pod UIDs, container IDs, image digests and nodes before the first preparation; if placement has changed, freeze and disclose a new block reference before producing data. Keep original consumers 0–2 alive across treatments where possible; merely reusing pod names does not prove pod identity.

The minimum implementation before running is a fail-closed comparison gate immediately before the runner publishes `state='running'`: normalize generated topic names to topic indices; compare all 60 ownership entries, initial replicas, current original pod identities and node/resource observations against the block reference; re-read status and placement after monitoring readiness; reject missing or changed data. The startup reference file and its hash belong in the run manifest. Fresh observations must also confirm the reference before the scheduled action; an unexpected original-pod replacement or pre-action ownership change invalidates the controlled treatment.

This check **validates** an assignment; it does not force one. A bounded preparation attempt may restart application processes through the normal coordinator before any workload starts. Proposed limit: six preparation attempts per treatment and 15 minutes total preparation per block, whichever comes first. Keep every failed preparation and its reason. Select on the predeclared identity/map checks only, never on favorable latency or backlog outcomes. If the reference cannot be reproduced, stop the controlled comparison and report the preparation failure. Do not retry until the results look favorable, silently relax the matching requirement, or label a differently placed run matched.

Changing to static membership or another assignor is not an automatic fix: membership retention, clean shutdown and fresh-topic behavior would need their own validation. It is outside this bounded repeat design.

## Later RQ3 experiment: two feasible busy partitions sharing one consumer

Once a supported transfer is implemented and its handover validated, start with one condition and three treatments: keep three, transfer one intact busy partition while keeping three, and scale three to six. Two randomized complete treatment blocks would produce six characterization runs. Use the same client, assignment mechanism, processing task and instrumentation for every treatment.

A concrete **uncalibrated candidate** uses 900 messages/s total (`TARGET_RATE='300'` per producer), 80% routed equally in expectation to partitions 0 and 3, and the rest across the other 58 partitions. Under the proposed reference map, both busy partitions start on consumer 0. Each receives about 360/s; each cold partition receives about 3.10/s. Consumer 0 receives about 775.86/s, while consumers 1 and 2 each receive about 62.07/s. These are target-demand calculations, not measured capacities.

The planned intact transfer is partition 3 from consumer 0 to consumer 1. Expected demand then becomes approximately 415.86/s, 422.07/s and 62.07/s across consumers 0–2. Partition counts become 19, 21 and 20. There is no producer-routing change and no key splitting. If the selected assignor requires an additional cold-partition swap, declare it before the comparison and update the expected map, demand and affected-partition measurements. Do not describe a two-partition exchange as a one-partition move.

Calibrate the actual source and eligible destination with the same task, cold-partition work and instrumentation. A proposed qualification uses destination loads no greater than 80% of a conservative observed sustainable rate and source demand at least 120% of its calibrated sustainable rate, plus observed positive backlog growth during an independent no-action qualification. Both busy partitions must be individually feasible and the combined post-transfer assignment must have measured headroom. Freeze the resulting rate before treatment blocks. The previous one-node 639/s observation is insufficient to approve this candidate.

Use 600 s production, including 60 s warm-up, and 120 s drain as the starting timing design; action at production +120 s. That leaves 480 s of continued input after the action. No recovery is promised. If calibration shows this horizon inadequate, revise and freeze it for every treatment before comparison.

## Required validation and measurement

Before a reassignment trial, validate one transfer with unique identities and offsets; graceful revoke, commit acknowledgement, assignment and resume; failures during transfer; and recovery from an unsuccessful commit. Verify exclusive ownership through the selected supported protocol. Record replayed work rather than assuming no duplicates. With the current stateless SHA task, compare output digests; external/stateful side effects require additional output validation. A successful healthy transfer alone is not exactly-once evidence.

Keep the existing whole-run cohort latency and nearest-rank p99 conditional on completion, unfinished counts/shares at the common drain cutoff, per-partition outcomes, acknowledged input and useful completions. Record requested consumer CPU-time over evaluation plus drain, not monetary or whole-cluster cost. Show recovery while input continues separately from later drain completion. Retain the 90% aggregate-lag coverage screen, resource coverage, freshness gaps and censored recovery. The 99 ms deadline remains provisional.

For each affected partition, record action request/completion, revoke start, last old-owner completed offset, commit acknowledgement/offset, new-owner assignment/start offset, first new-owner processing start/completion and every actual owner change. Report requested and actual moved-partition sets. The current logs provide part of this evidence; a new transfer command, protocol acknowledgement and partition-specific interruption analysis remain to be added.

Separate request-to-assignment, assignment-to-first-useful-completion, and the old-to-new completion gap. The last gap includes ordinary processing/interarrival time; establish queued work at the transition and report an unaffected-partition/no-action reference rather than calling every gap a pause. Same-process intervals can use a monotonic clock. Cross-node event differences need clock-error bounds; coordinator observations at five-second resolution are interval-censored and must not be presented as millisecond-accurate handover times. Keep raw timestamps, sampling intervals, missing events and censoring explicit.

Compare paired run outcomes first. Retain all valid unfavorable results. Do not average consumer p99 values or use millions of messages as independent experimental repetitions. A favorable scheduled reassignment establishes behavior for this condition, not superiority or novelty of an automatic controller. The unresolved CC-MWF full-text comparison remains a literature dependency.

## Prepared artifacts and remaining work

`review-plan.json` contains the two candidate configurations, reference ownership/placement, source hashes and local feasibility checks. It is intentionally a review document, not a `save-run.sh` action plan. Existing active runtime settings were not changed. No cluster campaign was started.

For the immediate priority, implement and test the placement/ownership gate, then validate preparation without producing an experimental workload. Only after that succeeds can the existing keep-three/scale comparison be called ready for a controlled repeat. Targeted reassignment remains a later mechanism project. The proposal already describes it as planned work; this assessment does not turn it into an implemented feature or remove optional hot-key splitting from RQ4.

## Implementation following the review

The subsequent bounded-repeat protocol uses the following implementation. The placement gate and preparation-only mode are now implemented; see [EXECUTION.md](EXECUTION.md) for the runnable block and its safeguards. This does not retroactively change the dated feasibility record in `review-plan.json`. Live validation and outcomes must be reported from execution evidence, not inferred from local tests.
