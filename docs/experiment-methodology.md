# Experiment methodology

The first experiments establish reliable measurements and the conditions in which consumer scaling is useful. They precede comparisons of a complete adaptive controller. A run that finishes successfully is useful operational evidence, but its scientific interpretation depends on the workload, comparison and measurement coverage.

## Define the question before the run

Write down the workload, the intervention, the no-action or competing treatment, the completion endpoint and the outcomes to compare. Record what would count as no benefit. For example, a scale-up comparison should establish whether improved completion or backlog recovery justifies additional consumer resource-time and startup/rebalance disruption.

Use a fixed topic partition count and consistent processing semantics. Keep workload parameters, seeds, per-consumer resources and observation windows comparable. Retain actual initial assignments because the group assignor is not forced to reproduce the same map in every run.

## Initial sequence

1. **Measurement screening:** inspect one balanced and one statically skewed input with no intervention. Check actual admission, completed work, partition demand, lag coverage and unfinished records.
2. **Capacity calibration:** choose a modest workload adjustment that produces sustained measurable backlog while leaving enough partition parallelism for additional consumers to help. A zero-work smoke test does not establish that condition.
3. **Scheduled comparison:** compare no action with a declared scale-up at the same evaluation-relative time. Include the delay until new capacity performs useful work.
4. **Repeat informative conditions:** repeat matched configurations and report variability. One run per condition is screening evidence, not a general performance conclusion.

A later boundary case isolates one overloaded partition. A later reassignment case deliberately places several busy partitions on one consumer while another has spare capacity. These must be verified from actual assignments; static skew alone does not prove that either condition occurred.

## Timing and workload

The current proposal table uses a provisional 5-minute warm-up, 5-minute pre-change interval, 15-minute post-change interval and bounded drain, with five pilot repetitions. The [next-run review folder](../experiments/next-run-review/README.md) also contains a shorter screening alternative. Any deviation must be stated with its purpose; shorter runs do not silently become the final protocol.

Intervention timing is relative to evaluation start, excluding warm-up. Bounded drain is part of observing outstanding work; records unfinished at its end remain explicitly unfinished. Select rate, work per record, duration and repetition count through bounded calibration before freezing a comparison. Record achieved admission separately from the requested rate.

## Minimum evidence to inspect

| Evidence | Interpretation |
|---|---|
| Acknowledged input and identity checks | Defines the cohort and detects inconsistent logical or physical identities |
| Completed throughput and latency distribution | Describes observed processing, using the declared endpoint |
| Unfinished and provisional deadline outcomes | Prevents completed-only latency from hiding outstanding work |
| Partition backlog, growth and assignments | Shows where demand and processing progress diverge |
| Coverage and missing observations | Defines which intervals support backlog and cost summaries |
| Consumer resource-time and process history | Includes resource declarations and removed/restarted instances within its stated scope |
| Action and recovery timeline | Distinguishes a request from readiness, useful completion and observed recovery |

Exact definitions are in [Metric definitions](metric-definitions.md). Returned-record offset lag and processing backlog are distinct. Offset spans are not always exact record counts. Resource-request integrals are not measured CPU use or whole-cluster cost.

## Comparisons and reporting

Retain per-run results, then group only compatible configurations and initial conditions. Report run-level quantiles and pooled message quantiles separately; averaging p99 values does not produce a pooled p99. A poor-performing but valid trial belongs in the results. An interrupted or invalid run needs an explicit status and reason.

If recovery is not observed within the follow-up window, report it as censored. If measurement coverage is inadequate, report unavailable evidence rather than inferring successful recovery. The current clock observations do not establish sufficiently tight accuracy for a definitive 99 ms deadline claim.

For the eventual controller comparison, use relevant scaling and workload-aware assignment baselines. Where the question concerns selection logic, match the permitted action set, observations and assignment procedure; otherwise the comparison may attribute a stronger actuator or additional resources to a better decision rule. Label adaptations of published methods as adaptations unless the original implementation and protocol are reproduced.

## Preserve provenance

Commit the completed code and reviewed configuration before a run, record the revision and any remaining edits, and retain the frozen configuration and runtime hashes. Keep small summaries and evidence hashes under `experiment-records/`; preserve raw evidence separately. Do not assign a later commit to an earlier experiment as though that revision was executed.
