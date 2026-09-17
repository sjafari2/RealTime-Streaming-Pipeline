# Kafka I/O diagnosis — 17 September 2026

Read-only follow-up in namespace `kafkastreamingdata`, approximately 10:49 UTC. No pods, storage, topics or settings were changed during this diagnosis.

## Confirmed observations

All three Kafka brokers are hosted on `patternlab.calit2.optiputer.net`. Their separate 300 GiB persistent claims use `rook-ceph-block-central`. Broker 0 mounts `/dev/rbd3` and broker 2 mounts `/dev/rbd4` at `/bitnami/kafka`, both with XFS.

| Observation | Broker 0 | Broker 2 |
|---|---|---|
| Metadata thread | `kafka-0-raft-io`: uninterruptible wait, `folio_wait_bit_common` | `kafka-2-raft-io`: uninterruptible wait, `xlog_wait` |
| Scheduler threads | XFS log waits | XFS log waits |
| Outstanding block I/O at both samples | 13 | 6 |
| Completed reads over ten seconds | 0 | 0 |
| Completed writes over ten seconds | 0 | 0 |
| Filesystem usage | 480 MiB / 300 GiB, reported 1% | 480 MiB / 300 GiB, reported 1% |

The earlier graceful restart of controller 1 remains unresolved: the pod is terminating without finalizers. The exporter terminated; the Kubernetes status still reports Kafka running, while a direct exec attempt reports the container stopped. This discrepancy needs kubelet/container-runtime inspection. Repeated Killing events and readiness timeouts were observed. No force deletion was attempted.

## Interpretation and limits

The immediate blocker is outstanding block-device I/O that is not completing, with Kafka metadata and scheduler threads waiting on filesystem operations. Full disk space is not indicated. Both affected volumes share a node and storage backend, so a shared infrastructure path is implicated. These observations do not establish whether the underlying cause is the node's kernel/RBD client, the node-to-Ceph network path, or Ceph backend health. They do not establish XFS corruption, a cluster-wide outage, or a fault caused by workload skew.

Namespace access cannot read nodes, PersistentVolumes or the storage-system pods. Therefore the final infrastructure root cause cannot be verified with the current permissions.

## Administrator investigation

Check kernel and kubelet/container-runtime logs on `patternlab.calit2.optiputer.net` for hung tasks, RBD/Ceph timeouts, XFS errors and shutdown failures. Inspect Ceph central health, slow operations, placement groups/OSDs, and connectivity from this node. Inspect the RBD CSI node plugin, attachments and mappings for these volumes:

- `data-pip-kafka-controller-0`: `pvc-b64a80c1-48e3-4dad-b040-47bdcc6e3d33`
- `data-pip-kafka-controller-1`: `pvc-b324217f-cf10-49ce-b4d3-c6b9f925474a`
- `data-pip-kafka-controller-2`: `pvc-e64d1902-df50-4c86-a51e-47727aca0c77`

Preserve existing volumes and Kafka data. Choose recovery after establishing where I/O is blocked. After recovery, verify completing disk requests, metadata quorum, fresh-topic creation and calibration before starting stability trials.

Disk-stat field interpretation follows the [Linux kernel documentation](https://docs.kernel.org/admin-guide/iostats.html). NRP's [storage troubleshooting guide](https://nrp.ai/documentation/admindocs/storage/volume-mounting/) describes administrator-level CSI/network investigation; its specific mount-failure examples are not a diagnosis of this incident.
