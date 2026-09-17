# Limitations and research roadmap

The repository provides an instrumented Kafka pipeline and 24 preliminary performance trials comparing keep-three with scheduled scale-to-six. See [current status](current-status.md) and the [experiment register](../EXPERIMENT_REGISTER.md). The complete adaptive decision policy has not been evaluated.

## Implemented capabilities

- Managed producer and consumer execution with a frozen configuration and shared run schedule.
- Persistent acknowledgement/completion evidence and run-scoped monitoring export.
- Whole-run and partition-level outcome analysis, lag coverage, resource history and repetition summaries.
- Scheduled no-action and consumer scale-up pilots with recorded plans and observed events.

Consult the active branch's tests and [experiment records](../experiment-records/README.md) for the scope of validation. Local tests establish specific code behavior; they do not prove live-cluster performance.

## Current evidence limits

Initial smoke tests checked collection and reconciliation. The first 16 performance trials recorded ownership and machine placement without requiring them to match. The later eight verified matching starting ownership and placement of the original three consumers. All valid outcomes were retained. Scaling helped in both later distributed 80/20 comparisons, while the matched single-partition outcomes were mixed. Limited repetitions, shared-machine variability and monitoring gaps limit interpretation. No universal capacity or policy superiority is established, and the 99 ms deadline remains provisional.

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

These stages are research dependencies, not promised completion dates. The [dated campaign protocols](../experiments/preliminary-campaign-20260912/README.md) and completed run inventory supersede the earlier next-configuration review as the record of what was executed. Targeted reassignment has experimental code paths but still requires live validation and controlled performance evaluation. The immediate next stage is the six pending balanced stability trials; later stages include moving hotspots and variable record costs.

## Decisions still open

The representative application and durable completion endpoint, numerical performance target or resource budget, reassignment protocol, fallback behavior and final experiment durations require explicit choices. A numerical SLA/SLO remains provisional until its application basis and measurement accuracy are established. Current comparisons therefore report latency and resource trade-offs directly.

The intended contribution is an experimentally supported account of when the decision policy improves performance, when it does not, and what its actions cost relative to the selected baselines.
