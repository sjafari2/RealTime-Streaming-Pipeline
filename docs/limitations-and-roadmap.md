# Limitations and next steps

The [24 current trials](../experiment-records/README.md) cover balanced-rate calibration and scheduled interventions under two skew patterns. Each condition was run twice. Earlier campaigns remain on the archive branch and are not pooled into this result set.

## Limits of the evidence

- Shared-machine capacity can vary despite matching starting ownership and original-consumer placement. Added consumers may run on different machines.
- The workload is synthetic and stateless. Completion is before commit acknowledgment; identity and offset checks do not establish exactly-once effects at an external application sink.
- The two targeted responses use a different coordination mechanism from native Kafka scaling. Their results compare complete implemented responses, not replica count alone.
- Requested CPU and memory over time are consumer-container costs. Actual process CPU/RSS are reported separately; neither measures the entire cluster or a monetary cost.
- The first twelve-partition baseline has insufficient resource-request coverage for a full-window requested-cost comparison. Its completed-message evidence remains usable. Missing measurements remain unavailable.
- Recovery requires backlog at or below 100 offsets for 30 seconds of valid observations while input continues. It does not guarantee continued stability. Experimental latency deadlines are not validated application SLAs.
- The four-partition block has no keep-three/no-action or native-scaling arm. It cannot establish that all possible three-consumer assignments behave alike.

## Planned evaluation

The next stage extends the evidence to hot-key splitting, selected combinations and execution order. Splitting will first be checked for routing correctness, output combination and application ordering constraints. Whole-partition redistribution and key splitting address different limits and will be evaluated separately before combination.

Order comparisons will, where feasible, reach the same final capacity and assignment while changing the sequence of actions. Repeated workload schedules will measure recovery, tail latency, unfinished work, interruption and resource cost. Calibration and evaluation schedules will be separate when developing the heuristic decision policy.

Policy comparisons will include explicitly configured CPU-based HPA, lag-based KEDA and suitable workload-aware assignment baselines with documented adaptations. Comparable information, permitted actions and resource budgets are needed to assess the policy itself. Larger deployments require fresh calibration and measurements of monitoring, decision and coordination overhead. Drug–target prediction and fire detection remain future application evaluations.
