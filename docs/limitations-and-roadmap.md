# Limitations and research roadmap

The repository currently provides an instrumented Kafka pipeline and managed experiment workflow. Its strongest evidence is about implementation behavior and the recorded smoke test. Policy effectiveness remains to be evaluated.

## Implemented capabilities

- Managed producer and consumer execution with a frozen configuration and shared run schedule.
- Persistent acknowledgement/completion evidence and run-scoped monitoring export.
- Whole-run and partition-level outcome analysis, lag coverage, resource history and repetition summaries.
- Scheduled no-action and consumer scale-up pilots with recorded plans and observed events.

Consult the active branch's tests and [experiment records](../experiment-records/README.md) for the scope of validation. Local tests establish specific code behavior; they do not prove live-cluster performance.

## Current evidence limits

The September 2026 smoke test completed its acknowledged cohort and retained evidence for analysis. It did not compare mitigation treatments or establish sustained overload, calibrated service capacity, a final application workload or an effective selector. Its lag and resource coverage were incomplete, and its clock probe did not establish timing accuracy adequate for a definitive 99 ms deadline claim. Exact figures and limitations belong to the saved run record.

The current completion endpoint follows synthetic work and precedes commit acknowledgement. Within-epoch offset checks and identity consistency are useful diagnostics; they do not establish a durable exactly-once external result or prove ordering across every ownership transition.

Resource accounting covers the implemented consumer process/request measurements. It does not yet represent total infrastructure cost. Missing observations remain unavailable, and a missed recovery within bounded follow-up is censored.

## Planned stages

| Stage | Main deliverable | Evidence needed to proceed |
|---|---|---|
| Monitoring and capacity | Balanced/skewed profiles and bounded capacity calibration | Valid cohort accounting, known work, adequate observation coverage |
| Individual scale-up actions | Matched no-action/scale-up comparisons | Useful parallelism, measured actuation intervals and repeated outcomes |
| Targeted reassignment | Supported whole-partition ownership transfer | Progress handover, required ordering/duplicate-effect checks and transfer cost |
| Adaptive selection | A rule selecting waiting, reassignment or scaling | Relevant competing policies, matched information/actions and separate tuning/evaluation |
| Robustness | Conditions of benefit, no benefit and failure | Bursts, changing demand/capacity and imperfect observations |
| Optional key splitting | Correct finer-grained work distribution | Compatible application semantics, combination cost and final-output evidence |

These stages are research dependencies, not promised completion dates. The immediate configuration is [prepared for review](../experiments/next-run-review/README.md); proposed runs are not recorded as completed experiments.

## Decisions still open

The representative application and durable completion endpoint, numerical performance target or resource budget, reassignment protocol, fallback behavior and final experiment durations require explicit choices. Keep a numerical SLA/SLO provisional until its application basis and measurement accuracy are established. Evaluate latency/resource trade-offs directly while that choice remains open.

The intended contribution is a supported account of when a decision rule helps and what it costs. Neither a different programming language nor the absence of an identical implementation name in prior work is sufficient evidence of novelty.
