# Idle resource cleanup — 14 September 2026

No new performance trial was started.

The consumer HPA was paused in both directions using UID and original-spec
preconditions. The consumer StatefulSet was scaled from six replicas to zero.
All six consumers had no running consumer application in the process checks.
Their temporary metrics directories were empty; their small final-status files
were saved locally before scale-down. Kubernetes is handling graceful termination;
a requested replica count of zero is not proof that all pods have terminated.

The original StatefulSet, HPA and PVC definitions and saved consumer status files
are in the local workspace at `review/idle-resource-cleanup-20260914/`.
Both StatefulSets retain PVCs when scaled or deleted. No PVC or Kafka topic was
deleted. Brokers, Prometheus and Grafana were left running.

Producer exec requests timed out, including a minimal echo command. The latest
producer-0 container log said it was waiting for manual start, but temporary-file
preservation could not be verified. The three producers therefore remain running.
This cleanup is partial; it does not establish namespace utilization compliance.
Broker CPU requests still require review against measured usage.

Before another experiment, inspect the saved HPA settings, restore the required
producer/consumer replicas, and verify shared storage and application readiness.
Keep the HPA paused during fixed/scheduled comparisons; restoring its original
minimum of six would interfere with a three-consumer baseline. Do not blindly
restore the saved spec over a controller changed by another operator.
