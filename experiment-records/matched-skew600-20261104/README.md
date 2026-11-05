# Matched four- and twelve-partition comparisons at 600 messages/s

This series repeats both traffic concentrations within one deployment. It contains 16 scheduled comparison trials, four responses at each concentration and two independent repetitions. A separate 20-minute balanced evaluation verifies the reference input rate before skew comparisons. No new outcomes are reported until the runs complete and pass validation.

All comparisons use 60 partitions, 600 messages/s total, 80% of input on four or twelve partitions, 2,000 SHA-256 iterations, and no artificial processing delay. The phases are 60 seconds of warm-up, 600 seconds of evaluation and 120 seconds of drain. Each action is requested at evaluation second 60. The high-traffic partitions initially belong to Consumer 0. The four-partition set is a subset of the twelve-partition set selected from the same initial ownership map.

The first repetition alternates twelve- and four-partition trials for each response. Both Keep 3 controls precede every mitigation. The second repetition reverses this order and uses seed 82 rather than 81. Seeds match across responses and workload concentrations within each repetition.

## Placement

The original three consumer and three producer identities, images, resource declarations and nodes must remain unchanged. Broker identities and placement must also match. Added consumers return to the same individual nodes on every scaling trial. Small temporary Deployment pods identify those nodes through required pod affinity matched on the StatefulSet pod-index label. Kubernetes still schedules the consumers and enforces their existing anti-affinity and resource requirements. The consumer image is pinned to the original running digest. The original pod template and update strategy are restored, and the temporary placement Deployments are removed after completion or failure.

Consumer 3 uses k8s-ravi-01, Consumer 4 uses k8s-chase-ci-01 and Consumer 5 uses unseenu. Existing consumers remain on k8s-chase-ci-04, k8s-chase-ci-10 and k8s-gen4-03. Full node names and the exact initial identities are retained in placement-plan.json. This does not reserve exclusive machines or eliminate shared-node contention.

## Admission and evidence

The balanced run requires full evaluation-message completion by drain cutoff, input within 1% of 600 messages/s and, in each of the last two five-minute evaluation intervals, at least 95% coverage, mean processing backlog no greater than 500 offsets and covered growth no greater than 1 offset/s. This is a finite-observation admission criterion, not a proof of indefinite stability.

Every Keep 3 control must demonstrate at least 100 offsets/s backlog growth and a mean total lag of at least 10,000 offsets in each of the final three one-minute evaluation intervals. Coverage must be at least 95%, with no invalid observation, ownership change or gap exceeding three seconds. These thresholds are fixed before new outcomes. A clear or insufficiently observed control stops the batch and remains part of the retained evidence. Input rate, processing cost and placement are not automatically adjusted to force stress. Valid unfavorable mitigation results remain reportable.

The managed runner preserves creation, acknowledgment, processing-start, completion, request, readiness, handover and activation timestamps, raw monitoring observations, clock-probe bounds and fixed phase boundaries. Raw records have a hash-verified second local copy outside Git. That second copy is on the same computer and is not an independent off-device backup. Figures will use separate performance, control-signal and resource-use categories, shared axes, short labels and observed two-run ranges. CPU and memory axes will be labelled "CPU use (cores)" and "Memory use (MiB)".

The implementation is in experiments/matched-skew600-20261104/run.py. Earlier experiments remain unchanged. No proposal results are replaced before this series is validated.

## Interrupted comparison and continuation

Five first-repetition trials completed and passed validation before the four-partition targeted-scaling attempt stopped at its unchanged 90-second enrollment deadline. Consumer 5 was scheduled on its recorded node but its application container had not started. The interrupted run is retained separately, including the available message and monitoring records. It is not counted as a completed trial. The original startup cause could not be established from the retained resource snapshots after Kubernetes events expired.

A subsequent no-traffic check brought all six pods to readiness on the same recorded nodes in approximately 17.4 seconds, then restored the original deployment. The partial-results file retains the five completed outcomes, both passed pressure checks, the balanced reference, and hashes for this diagnostic. These are individual first repetitions, not two-run averages. The four-partition redistribution outcome with unfinished work remains in the comparison.

The runner supports explicit continuation with both `--resume-series` and `--resume-campaign`. It verifies the unchanged design, calibration evidence, pressure checks, completed-trial prefix and original deployment before resuming only unfinished comparisons. It creates a separate series audit and backs up the prior campaign state. Startup limits, workload settings and node identities remain unchanged. Failure diagnostics now retain pod states and Kubernetes events before added pods are removed.
