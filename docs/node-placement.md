# Node placement in controlled experiments

Producer and consumer pods use `k8s/statefullsets/producer-sts.yaml` and `consumer-sts.yaml`. Their selectors are `app: producer-sts` and `app: consumer-sts` in `kafkastreamingdata`. The managed runner scales the existing consumer StatefulSet; it does not switch to a second deployment definition.

## Requirements for subsequent trials

Both templates require pod anti-affinity on `kubernetes.io/hostname` against **both** role labels. The default scope is the pod's own namespace. Each producer and consumer, including consumers added by scaling, therefore requires a different eligible node. The existing `us-west` region and `ucsd` zone constraints remain. The checked-in consumer baseline is three replicas.

The coordinator validates the deployed templates and actual pods before releasing traffic, and again before an intervention. It records original pod/container identities, nodes and resource declarations. Native scaling additionally waits for all target applications to report fresh, healthy status and stable, complete partition coverage, then verifies their node placement. Explicit scaling checks placement before ownership handoff and again after it. Changed original identities, shared producer/consumer nodes, incorrect labels, stale rules or failed readiness prevent acceptance of that comparison. The runner records failures; it does not silently relax the constraints, move pods or substitute a different intervention.

The run's `node-placement-checks.jsonl` records check stages, policy, observed nodes, relevant template affinity, namespace co-location and failures. Unschedulable pod conditions are included when available. Initial placement and policy are also retained in the manifest. API failures remain unavailable observations. Checks are made at these boundaries, not continuously: a passed check does not guarantee unchanged placement or load between observations.

## Optional broker separation

Kafka is identified by `app.kubernetes.io/name: kafka` in the same namespace, matching the supplied broker deployment. By default, producer and consumer templates **prefer** separation from these pods. Broker co-location is reported but does not reject a run.

For a study that requires it, render and deploy both client templates with `--separate-brokers`, and set `SEPARATE_BROKER_NODES: "true"` in the shared run configuration before starting. The runner requires the corresponding hard broker rule in both the template and actual pods, checks observed co-location and rejects missing broker observations. A mismatch between this option and the deployed broker rule is an error. This covers Kafka-labelled pods in this namespace, not brokers with other labels or in other namespaces. Verify those labels when adapting the deployment.

```bash
# Preview only; this does not contact or modify the cluster.
python3 my-shell/node_placement.py k8s/statefullsets/consumer-sts.yaml --separate-brokers
```

Deployment is a separate, between-run operation. It can replace pods and change their nodes; retained comparison references must be recaptured and empty-topic preparations repeated afterward. See the [deployment procedure](nautilus-measurement-update.md). With three producers, the three-consumer baseline needs six distinct eligible nodes and the six-consumer condition needs nine. Strict separation requires enough eligible nodes and resources; otherwise new pods may remain Pending and readiness fails. No automatic fallback enables co-location. An unsuccessful scale attempt retains its evidence and is reported as an intervention failure, not as a successful six-consumer trial. Poor performance with valid placement remains a valid outcome.

## Limits of the placement control

This is separation of specified experiment pods, **not exclusive machines**. Other projects' pods, pods in other namespaces, monitoring services, DaemonSets and other nonmatching workloads can share those nodes. These rules do not reserve a physical server, equalize processor models or processing capacity, eliminate resource contention, or guarantee permanent pipeline stability. Distinct nodes are also different from matching the exact nodes between comparisons: a saved starting reference checks the original pods, while added replicas may use different eligible nodes across trials. Their actual placement remains part of the evidence.

Kubernetes applies required anti-affinity when scheduling. Merely editing a local YAML file does not change an existing deployment or evict already placed pods. The independent runtime check catches an old or incomplete rollout before accepting traffic. The relevant semantics are described in the [Kubernetes scheduling documentation](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#inter-pod-affinity-and-anti-affinity).

## Historical configuration and observed trials

The August 4, 2025 [configuration change](https://github.com/sjafari2/RealTime-Streaming-Pipeline/commit/00f44902b0839365c7faaf62ae06d4f8d80dbbfa) replaced required pod anti-affinity with a required pin to `patternlab.calit2.optiputer.net`. The producer retained the former rule as comments. Strict separation conflicts with placing all these pods on one named node; this explains the technical incompatibility, but the commit does **not** document why that node was chosen. No claim about storage, capacity or debugging as the historical motivation follows from this evidence.

The November 8, 2025 [change](https://github.com/sjafari2/RealTime-Streaming-Pipeline/commit/f684a1922e4853086daf0022646d36052654ba61) replaced the single-node pin with UCSD/us-west locality and preferred anti-affinity. Consumers preferred other consumer nodes; producers preferred nodes without Kafka-labelled pods. Consumer topology spread used `ScheduleAnyway`. The recorded comment explains keeping clients near the Kafka brokers; it does not explain the earlier choice of a particular node. Saved September 2026 deployment snapshots confirm these soft rules. They were not turned off by the scaling action.

In the six scaling trials from the September 29–30 comparisons, original consumer 0 shared a node with producer 0, and original consumer 1 shared a node with producer 1. In every saved valid snapshot with six ready consumers, the six consumer nodes were distinct; added consumers did not share nodes with original consumers or the producers' recorded starting nodes. Added replica/node assignments varied between trials. This observation cannot establish continuous isolation across missing intervals or exclude unrelated workloads.

The [placement review](../experiment-records/node-placement-review.json) lists these run IDs, observed mappings, snapshot counts and hashes of the original evidence. It does not alter trial measurements, count new performance trials or retrospectively assert compliance with the new requirement. The new separation requirement changes the conditions for future comparisons, so those results must identify the new configuration and repeat calibration/start checks.
