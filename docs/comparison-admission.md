# Checks before a stressed comparison

The comparison runner requires a declared deployment reference and a successful Keep 3 stress check before testing mitigation responses. These checks apply to new executions through `experiments/four-condition-20260929/run.py`, including its twelve-partition and starting-backlog wrappers. Saved historical protocols and outcomes remain unchanged.

## Reference deployment

Supply `--admission-plan /path/to/plan.json` alongside the existing execution arguments. The plan names an earlier no-mitigation run with complete initial ownership and producer/consumer placement records. Its manifest and the plan are hashed, and the relevant values are saved in the new campaign protocol before execution.

The runner compares producer and consumer nodes, container image identities, resource requests and limits, and processing settings with that reference. It also checks that the selected initial hot-partition owner is on the reference bottleneck node. Recreated pod identities are allowed between campaigns. Producer and consumer placements are compared by role, so a changed consumer ordinal does not by itself imply a different machine. Within a campaign, the existing stricter checks still require matching pod identities, initial partition ownership and application readiness before traffic and before intervention.

A mismatch stops the campaign before the shared configuration is changed. Capturing the current placement does not make it an acceptable replacement for the declared reference. This check does not certify broker equivalence or equal available capacity on shared nodes. Existing resource and clock records remain necessary evidence, and the live stress check is still required.

## First Keep 3 run

The first Keep 3 performance trial must demonstrate sustained growth in total consumer-position lag during continuing input. The stress plan specifies consecutive windows at evaluation end, a minimum mean backlog and a minimum growth rate for every window. Each window uses the arithmetic mean of its valid backlog observations and the net change from its first to last observation divided by elapsed time. Warm-up and drain are excluded.

Coverage must meet the declared minimum. Missing or invalid observations, excessive gaps, duplicate times or changing ownership in the assessment period prevent admission. Neither insufficient monitoring nor nearly clear or draining backlog is interpreted as evidence of the intended stress condition.

If the check fails, the runner preserves the completed trial, writes `stress-admission.json` and `attention-required.json`, prints the stop reason, restores the original configuration and stops the batch. No subsequent mitigation trial is dispatched. The runner does not automatically increase the input rate, change processing cost or move pods to obtain overload. Report the observed measurements before choosing a new experimental plan. A stopped campaign cannot resume under different thresholds.

A comparison that omits Keep 3 runs a separate Keep 3 screening trial first. That screening is recorded separately from its planned performance-trial count. Empty-topic preparations and the existing low-input native-scaling technical check remain technical checks, not mitigation performance comparisons.

## Plan format

The following is a format example, not an approved launch plan or a validated controller threshold. Choose and record the reference and thresholds before starting the next workload. All stress fields are required, with no implicit defaults.

```json
{
  "schema_version": 1,
  "purpose": "stressed_skew_comparison",
  "reference_run": "results/run-20261027-004457",
  "hot_owner": "consumer-sts-0",
  "stress": {
    "window_seconds": 60,
    "consecutive_windows": 3,
    "minimum_growth_offsets_per_second": 10,
    "minimum_mean_backlog_offsets": 1000,
    "minimum_coverage": 0.95,
    "maximum_gap_seconds": 3
  }
}
```

For a 600-second evaluation, this example tests seconds 420–480, 480–540 and 540–600. A valid non-growing control remains a scientific result, but it does not qualify the deployment for this deliberately stressed comparison. Retain earlier proposal results until the intended comparable replacement series is complete.

## Verification

Offline tests exercise the full batch stop/restoration path with a clear first control, placement and processing mismatches, invalid monitoring, and resume protection. A read-only replay using the example thresholds rejects both nearly clear November 4 controls. The earlier October 27 control demonstrates sustained growth under the same replay settings. Replaying these checks does not change the validity or classification originally recorded for any trial and is not a new cluster experiment.

## Standalone Keep 3 on a declared new deployment

New skewed trials initially place all high-traffic partitions on Consumer 0. Mitigation may change their ownership later. Historical assignments remain unchanged.

`experiments/twelve-hot600-20261104/run_keep3.py` runs one control at 600 aggregate messages/s using a separately declared new deployment. Its plan records exact current pod identities, node placement, resources, processing settings, the earlier deployment differences and the stress criteria before traffic. An empty-topic preparation verifies readiness and captures ownership. Twelve partitions owned by Consumer 0 become the high-traffic set, and the no-action run verifies that map again before input. The command always stops after this one control, restores the shared configuration and preserves evidence, whether or not the stress check passes. It does not launch mitigation trials or claim that changed nodes match an earlier deployment.
