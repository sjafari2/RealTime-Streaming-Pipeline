# Stability campaign recovery — 15 September 2026

No new calibration or performance trial started. The completed comparison count remains 24.

## Findings

- Namespace queries and pod logs work using the current Nautilus identity.
- Remote echo commands in existing producers timed out. WebSocket connection establishment succeeded, but no command result arrived; alternate transport also timed out. Their cause is not yet proven.
- Six old consumer pod objects had terminated containers and pending deletion. Saved their metadata and removed only those objects using UID preconditions. Persistent volumes were retained.
- Starting one fresh consumer exposed a separate, explicit storage error: `/config`, backed by `pipeline-configmap-pvc` (PV `pvc-b85abb1b-ccd9-498e-902b-ae69a12c7e5a`, storage class `rook-cephfs-central`), could not mount on `k8s-chase-ci-01.calit2.optiputer.net`. Kubelet reported `DeadlineExceeded`, followed by `Aborted: an operation with the given Volume ID ... already exists`. The init container never started.
- The disabled HPA still enforced its minimum of six after replicas were raised from zero. Saved the controller specification and changed its minimum to one with UID/spec preconditions. Both scaling directions remain Disabled. Scaled back to one during diagnosis, then zero after collecting evidence.
- A temporary BusyBox pod without PVCs started on another node and returned `exec-ok` through kubectl exec. This demonstrates working credentials and remote execution on that pod; it does not establish that every node or volume is healthy. The diagnostic pod was deleted.
- Reading cluster-scoped node health was forbidden by RBAC. No cluster-wide repair was attempted.

## Preserved state and next action

Kafka brokers, producer pods, topics, PVCs and existing results were not deleted or reset. Consumers target zero replicas. The autoscaler remains paused, with minReplicas 1 and maxReplicas 12. Do not restore a minimum of six while the experiment environment is idle.

The next infrastructure step is to resolve the shared CephFS configuration-volume mount failure. If it persists, provide NRP support with the namespace, PVC/PV, node and exact mount errors above. No support message has been sent. Do not delete the PVC to work around this failure.

After storage and producer access recover, finish the six-trial execution wrapper, verify source deployment and evidence storage, run capacity calibration, and verify metric coverage and starting conditions before releasing traffic. These are pending tasks, not completed validation.

Local diagnostic snapshots are preserved outside the active runtime at:
`/Users/soheila/Documents/ChatGPT/Stream Processing Project/review/consumer-recovery-20260915/`.
