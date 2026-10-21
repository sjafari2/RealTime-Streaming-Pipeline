# Kubernetes deployment and source updates

The pipeline uses shared producer, consumer and configuration volumes. `/config/pipeline-configmap.yaml` holds the configuration for the next run; each managed experiment saves its own copy. The supplied manifests describe the research deployment on Nautilus and require adaptation for another namespace, storage class or image registry.

## Deployment components

| Location | Purpose |
|---|---|
| `k8s/kafka/` | Kafka brokers, storage and JMX monitoring configuration |
| `k8s/statefullsets/` | Producer and consumer pods and mounted application code |
| `k8s/pvc/` | Persistent volumes; existing claim names may differ from original templates |
| `k8s/services/` | Application and monitoring services |
| `k8s/grafana-prometheus/` | Standalone monitoring deployment |
| `dockerfiles_confluent/` | Images and pinned runtime dependencies |

Existing volumes contain configuration, source and research evidence. Reapplying an old storage template is not a source update. Deployment names and mounts are checked against the live resources before applying manifests.

## Updating a configured deployment

These commands run from the repository root on the coordinating machine:

```bash
kubectl config current-context
kubectl -n kafkastreamingdata get statefulset,pod,pvc,service
bash my-shell/save-run.sh backup
bash my-shell/save-run.sh stop
bash my-shell/sync-code.sh
```

`sync-code.sh` copies application and shared runtime files to the role volumes and checks their hashes through all current replicas. It requires stopped applications. It does not rebuild images, change Kafka, or overwrite the shared YAML. An existing run's evidence is collected before starting a new one.

The runtime dependencies are pinned in `dockerfiles_confluent/runtime-requirements.txt`. Imports can be checked inside each role:

```bash
kubectl -n kafkastreamingdata exec consumer-sts-0 -c consumer-container -- \
  python3 -c 'import confluent_kafka, prometheus_client, psutil, yaml; print(confluent_kafka.version(), confluent_kafka.libversion())'
kubectl -n kafkastreamingdata exec producer-workers-0 -c producer-container -- \
  python3 -c 'import confluent_kafka, prometheus_client, psutil, yaml; print(confluent_kafka.version(), confluent_kafka.libversion())'
```

A successful check prints client and library versions without an import error. Actual image digests and package versions belong in the run record.

## Configuration and startup

The local `src/pipeline-configmap.yaml` is a reference template: 700 aggregate messages/s across three producers, three consumers, one-minute warm-up, ten-minute evaluation and two-minute drain. `TARGET_RATE` is per producer; `EXP_DURATION_SEC` includes warm-up plus evaluation. Campaign protocols retain their own original settings.

The shared YAML is changed while applications are stopped. Bootstrap address, namespaces, volume paths, replica counts and resource declarations must agree with the deployment. Topic retention must cover preparation, production, drain and the required margin. The run preflight checks these constraints.

The consumer StatefulSet starts `supervise.py`. The supervisor waits for an active managed run and launches a consumer, including in a newly added replica. The producer application is started by the coordinator. Source synchronization must precede any rollout that needs newly added runtime files. Removing a volume or its initialization marker is not part of this workflow.

## Applying the placement requirement

`k8s/statefullsets/producer-sts.yaml` and `consumer-sts.yaml` are the authoritative client manifests. Both require separate nodes for producer and consumer pods, including replicas added by scaling. This is a new condition for subsequent trials; previous results retain their recorded placement.

After stopping applications, collecting the preceding evidence, synchronizing source and reviewing the manifests against the deployed volumes and other settings, apply both roles:

```bash
bash my-shell/deploy-sts.sh producer
bash my-shell/deploy-sts.sh consumer
kubectl -n kafkastreamingdata rollout status statefulset/producer-workers --timeout=180s
kubectl -n kafkastreamingdata rollout status statefulset/consumer-sts --timeout=180s
bash my-shell/save-run.sh preflight
```

This rollout can replace pods and change their nodes. Keep `SEPARATE_BROKER_NODES: "false"` for the default broker preference. If broker separation is part of the declared design, pass `--separate-brokers` to **both** deployment commands and set that shared configuration value to `"true"`. Do not change it during a run. The runtime checks require the deployment and saved configuration to agree.

A strict placement that cannot be scheduled remains Pending; inspect pod conditions and available eligible nodes instead of relaxing the rule within a comparison. A passing preflight checks templates, configuration and source; the managed start additionally checks actual pods before workload release. Recapture any matching-start reference and repeat empty-topic preparations after a rollout. The [placement guide](node-placement.md) distinguishes separation from machine exclusivity and documents the earlier trials.

## Monitoring

The supplied standalone Prometheus configuration supports dynamic pod discovery. An existing Prometheus Operator deployment uses its own discovery configuration instead. Metrics must cover every producer and consumer incarnation, including added replicas, before accepting an experiment. Broker JMX monitoring is separate from application metrics.

```bash
bash my-shell/port-forward-prometheus-grafana.sh
```

This helper opens Prometheus at `http://localhost:9090` and Grafana at `http://localhost:3000` for the supplied services. The dashboard is `grafana/KafkaDashboardPerConsumerMetrics.json`. Full-run commands can establish their own temporary Prometheus connection; `PROM_URL` selects an existing endpoint.

The [runtime guide](runtime-and-data-flow.md) covers execution and collection. The [experiment protocols](../experiments/README.md) add matched-start preparations and technical checks for the published comparisons. Local tests do not establish cluster readiness, node clock accuracy or live processing capacity.

## Client resource sizing and idle cleanup

The client manifests use the following allocations used in the current capacity calibration:

| Role | CPU request / limit | Memory request / limit |
|---|---|---|
| Producer | 300m / 360m | 160 MiB / 192 MiB |
| Consumer | 2 / 2.4 cores | 768 MiB / 920 MiB |

The bootstrap init containers explicitly request 100m CPU and 128 MiB, with limits of 120m and 152 MiB. Without these declarations, the namespace supplies a 1 GiB init-container request, which would prevent the intended pod memory reduction.

Producer CPU requests and both roles’ memory requests remain below the historical 2 CPUs and 1 GiB per client; the consumer CPU request now matches the historical value. The October 20 short calibration used producer requests of 250m CPU and 128 MiB, and consumer requests of 1 CPU and 512 MiB. An extended check with consumer requests of 1.25 CPUs and 640 MiB was stopped early after sustained backlog growth. A second check raised consumer requests to 2 CPUs and 768 MiB; it was also stopped early after 2.43 minutes of evaluation because backlog continued to grow. Consumer CPU throttling was absent in the sampled interval. The [October 21 calibration record](../experiment-records/resource-calibration-20261021/README.md) preserves both attempts and their actual durations. Producer requests remain 300m CPU and 160 MiB because the producers achieved the target rate. These are separate resource conditions, not changes to completed run records. Across the twelve September 29–30 intervention trials, the largest sampled process usage was 1.055 cores and 275.71 MiB for consumers, and 0.25 cores and 32.94 MiB for producers. Those measurements exclude some container overhead and do not bound unobserved peaks. The lower allocations therefore require calibration of achieved input, processing capacity, container memory, CPU throttling and restarts before comparative trials. Limits remain within 20% of requests, as specified by the [Nautilus resource policy](https://nrp.ai/documentation/userdocs/start/policies/).

Historical run records retain their original allocations. A new campaign records its deployed requests, limits and placement separately and holds them fixed within the comparison. Kafka brokers are sized separately: their JVM heap, off-heap memory and page cache are not represented by the Python process measurements. The active broker allocations have not been reduced.

Stopping the Python applications alone does not release the pods' reserved CPU and memory. After collecting evidence and stopping the managed run, idle client StatefulSets can be reduced to zero:

```bash
bash my-shell/save-run.sh stop
kubectl --context nautilus -n kafkastreamingdata scale statefulset consumer-sts --replicas=0
kubectl --context nautilus -n kafkastreamingdata scale statefulset producer-workers --replicas=0
```

Check that the live StatefulSets retain their PVCs when scaled, that autoscaling is paused, and that no run is active before cleanup. Application data and configuration remain on the volumes; deleting PVCs is not part of idle cleanup. Scale the clients back to the declared starting count only when preparing the next campaign, then verify readiness, source, resource declarations and placement. An unreachable-node pod that remains Terminating requires a safe recovery check; do not force-delete it merely to clear the display.

The [October 20 resource record](../experiment-records/resource-sizing-20261020/README.md) preserves the short calibration and replacement-deployment evidence. Subsequent calibrations retain their own configuration, resource declarations and placement records.


## Producer workload identity

The producer manifest supplies `PRODUCER_WORKLOAD_ID` from `metadata.labels['apps.kubernetes.io/pod-index']`. This is a stable ordinal (`0`, `1`, `2`), independent of the StatefulSet name. The [Kubernetes downward API](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/) exposes this label to the process. Keep the same producer count and ID mapping within a comparison.

This change requires both the updated source and the producer pod environment. Source synchronization alone does not update existing pod environments. Between runs, deploy the reviewed producer manifest through the normal recovery/setup procedure and verify the IDs with `bash my-shell/save-run.sh preflight`. A missing label produces an empty ID and fails validation; there is no hostname fallback. Do not put `PRODUCER_WORKLOAD_ID` in the shared configuration, which all replicas load.

For standalone troubleshooting inside the producer application directory, provide an explicit ID if the pod environment does not already supply one:

```bash
PRODUCER_WORKLOAD_ID=0 PIPELINE_STANDALONE=true bash run.sh
```

Use different IDs for simultaneous producers. In managed runs, preflight checks the pod environments and the start barrier checks the running applications again. The manifest's `producer_workloads` map and each producer's status/configuration event retain the actual ID, seed, RNG scheme and derived seed. The October 20 replacement passed the deployed source and workload-ID checks and completed a separate [resource calibration](../experiment-records/resource-sizing-20261020/README.md). Its short duration does not establish sustained stability; later source changes still require synchronization and deployment checks. See [experimental methodology](experiment-methodology.md#reproducible-producer-streams) for the distinction from historical hostname-seeded runs.


## Replacement producer deployment

The active producer StatefulSet is `producer-workers`, with pods `producer-workers-0` through `producer-workers-2`. The existing manifest filename remains `producer-sts.yaml`. `my-shell/placement_control.py` defines the deployment selectors used by discovery, readiness, ownership checks and HPA isolation. The producer headless Service now selects this deployment; monitoring must discover these pod names before a run is accepted.

The replacement uses `producer-data-20261020` and shares `pipeline-config-20261020` with the consumers. Their mounts remain `/app/producer-data` and `/config`, respectively. The retired `producer-sts-1` and its original claims are retained for administrator recovery and are not part of new run membership. The new deployment does not reuse that pod name, source volume or configuration volume. Original run evidence remains associated with its recorded deployments and resources.

Create the claims from `k8s/pvc/client-recovery-20261020.yaml` only when preparing this replacement. Seed the new configuration with a stopped run control and the reviewed calibration settings, synchronize the current source, and verify all hashes before releasing traffic. Preserve existing consumer locality exclusions when updating its template. The replacement producer IDs remain `0`, `1`, and `2`; deployment names do not seed their random streams.
