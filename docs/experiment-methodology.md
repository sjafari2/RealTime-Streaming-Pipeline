# Experiment methodology

The experiments establish reliable measurements and compare the conditions in which scaling and partition redistribution are useful. They precede comparisons of a complete adaptive controller. A run that finishes successfully is useful operational evidence, but its scientific interpretation depends on the workload, comparison and measurement coverage.

## Experimental design

Each comparison specifies the workload, intervention, baseline treatment, completion endpoint, and outcomes before execution. The evaluation includes conditions in which an action provides no benefit. Scaling comparisons assess completion and backlog recovery alongside additional consumer resource-time and startup/rebalance disruption.

The protocol uses a fixed topic partition count and consistent processing semantics. Workload parameters, seeds, per-consumer resource declarations, and observation windows are recorded for comparability. Actual initial assignments are retained because the group assignor can produce different maps across runs.

## Calibration and comparison procedure

1. **Measurement screening:** inspect one balanced and one statically skewed input with no intervention. Check actual admission, completed work, partition demand, lag coverage and unfinished records.
2. **Capacity calibration:** identify an input rate that has low bounded backlog under balanced input over the observed interval, then verify whether concentrated ownership creates a bottleneck at that rate. Separate pressure tests identify overload conditions. A zero-work smoke test does not establish processing capacity.
3. **Scheduled comparison:** compare no action with declared scaling and redistribution treatments at the same evaluation-relative time. Include startup, transfer, processing interruption and recovery while production continues.
4. **Repeat informative conditions:** repeat matched configurations and report variability. One run per condition is screening evidence, not a general performance conclusion.

Completed boundary cases include input concentrated on one partition and several hot partitions concentrated on one consumer. Actual ownership and observed processing capacity are essential: static input skew alone does not establish overload. The latest four-condition block verifies a common starting map and original-consumer placement, uses the same hot set for every condition, and reverses condition order in Run 2. Native Kafka scaling and explicit redistribution use different coordination mechanisms, which remain part of the interpretation.

## Timing and workload

Completed trials use one minute of warm-up and two minutes of drain. Original scaling comparisons lasted 5, 7 or 12 minutes total; balanced stability trials lasted 23 minutes; the first ownership and redistribution blocks lasted 8 minutes; the latest four-condition block lasted 13 minutes. Their evaluation durations are therefore 2, 4, 9, 20, 5 and 10 minutes, respectively. Use each saved manifest for exact boundaries.

Warm-up messages are excluded from the evaluation cohort, but their outstanding work remains queued. A Run 1 or Run 2 label identifies the corresponding separate trial for each condition, not before-and-after phases of one trial. The [completed stability inventory](../experiment-records/stability-fixed3-20260921/COMPLETED_RESULTS.md) supersedes the earlier six-trial plan as the record of what actually ran.

Intervention timing is relative to evaluation start, excluding warm-up. Bounded drain is part of observing outstanding work; records unfinished at its end remain explicitly unfinished. Select rate, work per record, duration and repetition count through bounded calibration before freezing a comparison. Record achieved admission separately from the requested rate.

## Evidence retained for interpretation

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

Per-run results are retained and compatible configurations are grouped for repetition analysis. Run-level quantiles and pooled message quantiles are reported separately; averaging p99 values does not produce a pooled p99. Valid unfavorable outcomes remain in the results. Interrupted or invalid attempts have a separate status and reason.

If recovery is not observed within the follow-up window, report it as censored. If measurement coverage is inadequate, report unavailable evidence rather than inferring successful recovery. The current clock observations do not establish sufficiently tight accuracy for a definitive 99 ms deadline claim.

For the eventual controller comparison, use relevant scaling and workload-aware assignment baselines. Where the question concerns selection logic, match the permitted action set, observations and assignment procedure; otherwise the comparison may attribute a stronger actuator or additional resources to a better decision rule. Label adaptations of published methods as adaptations unless the original implementation and protocol are reproduced.

## Preserve provenance

The experiment records identify the execution revision, any remaining source differences, the frozen configuration, and runtime hashes. Small summaries and evidence hashes are versioned under `experiment-records/`; raw evidence is preserved separately. Later source or documentation changes retain the original execution provenance.
