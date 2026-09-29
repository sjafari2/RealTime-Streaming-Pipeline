# Limitations and research roadmap

The repository provides an instrumented Kafka pipeline and **56 performance trials** spanning rate calibration, ownership and scheduled mitigation. See [current status](current-status.md) and the [experiment register](../EXPERIMENT_REGISTER.md). The adaptive decision policy and optional hot-key splitting remain unimplemented.

## Implemented and evaluated capabilities

- Managed producers and consumers with a saved configuration and common timing boundaries.
- Persistent acknowledgment/completion evidence, run-scoped monitoring and identity/offset checks.
- Whole-run and partition-level outcomes, growth, skew, coverage, process CPU/RSS and requested resource-time.
- Scheduled native scaling, explicit whole-partition redistribution, and their combined treatment, with recorded transitions and recovery.

Local tests establish specific code behavior. The [experiment records](../experiment-records/README.md) identify the live technical checks and performance trials that support each implemented response.

## Evidence limits

The original scaling block contains twenty-four trials. Its first sixteen recorded ownership and placement without requiring matching starts; the later eight verified the initial ownership and original-consumer placement. Matched single-partition scaling outcomes were mixed. Later balanced calibration and 700-message/s ownership experiments address a different question: how a rate that is sustainable under balanced input can overload one assigned consumer.

The latest four-condition block has two trials per condition with verified common starts. Kafka's native rebalance and the explicit handoff use different coordination mechanisms, so the comparison evaluates the implemented responses, not replica count in isolation. Matching original pods and ownership does not eliminate changing load on shared machines or variability in newly added consumers. No universal capacity limit, permanent stability or adaptive-policy superiority is established.

The completion endpoint follows synthetic application work and precedes commit acknowledgment. Identity, offset-continuity and handoff checks do not establish exactly-once external effects or correctness for arbitrary stateful applications. The 99 ms deadline is provisional; its application basis and clock accuracy require further validation.

Resource accounting separates process CPU/RSS from requested consumer resources over time. Neither is total cluster or economic cost. The first baseline in the latest block has incomplete local resource-request observations after the coordinating Mac slept. Its historical monitoring and outcome evidence were recovered without rerunning traffic; missing request intervals remain unavailable.

The offset-initialization monitoring correction does not fill historical gaps or remove legitimate interruptions during handoff. Recovery means the specified threshold and holding interval were observed while production continued; it is not a guarantee of indefinite stability. Sensitivity to recovery thresholds is retained.

## Next research stages

| Stage | Deliverable | Evidence needed |
|---|---|---|
| Capacity-aware redistribution | Assignment choices informed by measured processing capacity | Repeated comparisons against simpler balanced and native assignments |
| Adaptive decision policy | A rule choosing waiting, redistribution or scaling | Separate tuning/evaluation schedules and comparable information, actions and resource budgets |
| Robustness and scale | Limits under moving hotspots, bursts, changing costs and larger deployments | Valid measurements, intervention overhead and unfavorable outcomes |
| Optional hot-key splitting | Finer-grained work distribution where semantics permit | Routing, combination, ordering and final-output correctness |
| Application evaluation | Representative stateless processing workloads | A justified completion endpoint, performance targets and application-specific validation |

The immediate evidence does not justify declaring one response universally best. Latency, unfinished work, recovery, interruption and resource cost can favor different choices. Future evaluation will test whether a decision policy improves that trade-off compared with the selected baselines. Existing whole-partition handoff is implemented; broader fault handling and stateful transfer are separate research problems.
