# Experiment methodology

The first experiments establish reliable measurements and the conditions in which consumer scaling is useful. They precede comparisons of a complete adaptive controller. A run that finishes successfully is useful operational evidence, but its scientific interpretation depends on the workload, comparison and measurement coverage.

## Experimental design

Each comparison specifies the workload, intervention, baseline treatment, completion endpoint, and outcomes before execution. The evaluation includes conditions in which an action provides no benefit. Scaling comparisons assess completion and backlog recovery alongside additional consumer resource-time and startup/rebalance disruption.

The protocol uses a fixed topic partition count and consistent processing semantics. Workload parameters, seeds, per-consumer resource declarations, and observation windows are recorded for comparability. Actual initial assignments are retained because the group assignor can produce different maps across runs.

## Calibration and comparison procedure

1. **Measurement screening:** inspect one balanced and one statically skewed input with no intervention. Check actual admission, completed work, partition demand, lag coverage and unfinished records.
2. **Capacity calibration:** choose a modest workload adjustment that produces sustained measurable backlog while leaving enough partition parallelism for additional consumers to help. A zero-work smoke test does not establish that condition.
3. **Scheduled comparison:** compare no action with a declared scale-up at the same evaluation-relative time. Include the delay until new capacity performs useful work.
4. **Repeat informative conditions:** repeat matched configurations and report variability. One run per condition is screening evidence, not a general performance conclusion.

A later boundary case isolates one overloaded partition. A later reassignment case deliberately places several busy partitions on one consumer while another has spare capacity. These must be verified from actual assignments; static skew alone does not prove that either condition occurred.

## Timing and workload

Completed comparisons used one minute of warm-up and two minutes of drain. Their total durations were 5, 7 or 12 minutes, leaving 2, 4 or 9 minutes of evaluation production. Warm-up messages are excluded from the evaluation cohort, but their outstanding backlog remains in the pipeline. Each category has Run 1 and Run 2, each containing separate keep-three and scale-to-six trials.

The next [six stability trials](../experiments/stability-20260914/README.md) are planned for 23 minutes total: 1 warm-up, 20 evaluation and 2 drain. They have not started. Use each saved run manifest for actual boundaries; older review-only schedules are not descriptions of completed experiments.

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
