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
kubectl -n kafkastreamingdata exec producer-sts-0 -c producer-container -- \
  python3 -c 'import confluent_kafka, prometheus_client, psutil, yaml; print(confluent_kafka.version(), confluent_kafka.libversion())'
```

A successful check prints client and library versions without an import error. Actual image digests and package versions belong in the run record.

## Configuration and startup

The local `src/pipeline-configmap.yaml` is a reference template: 700 aggregate messages/s across three producers, three consumers, one-minute warm-up, ten-minute evaluation and two-minute drain. `TARGET_RATE` is per producer; `EXP_DURATION_SEC` includes warm-up plus evaluation. Campaign protocols retain their own original settings.

The shared YAML is changed while applications are stopped. Bootstrap address, namespaces, volume paths, replica counts and resource declarations must agree with the deployment. Topic retention must cover preparation, production, drain and the required margin. The run preflight checks these constraints.

The consumer StatefulSet starts `supervise.py`. The supervisor waits for an active managed run and launches a consumer, including in a newly added replica. The producer application is started by the coordinator. Source synchronization must precede any rollout that needs newly added runtime files. Removing a volume or its initialization marker is not part of this workflow.

## Monitoring

The supplied standalone Prometheus configuration supports dynamic pod discovery. An existing Prometheus Operator deployment uses its own discovery configuration instead. Metrics must cover every producer and consumer incarnation, including added replicas, before accepting an experiment. Broker JMX monitoring is separate from application metrics.

```bash
bash my-shell/port-forward-prometheus-grafana.sh
```

This helper opens Prometheus at `http://localhost:9090` and Grafana at `http://localhost:3000` for the supplied services. The dashboard is `grafana/KafkaDashboardPerConsumerMetrics.json`. Full-run commands can establish their own temporary Prometheus connection; `PROM_URL` selects an existing endpoint.

The [runtime guide](runtime-and-data-flow.md) covers execution and collection. The [experiment protocols](../experiments/README.md) add matched-start preparations and technical checks for the published comparisons. Local tests do not establish cluster readiness, node clock accuracy or live processing capacity.
