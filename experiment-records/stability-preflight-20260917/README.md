# Stability preparation — 17 September 2026

No calibration workload or stability performance trial was released. This is an infrastructure/readiness record, not experimental evidence of workload instability.

Three producers and three consumers passed dependency and source-hash checks. All 11 shared runtime files were synchronized and verified. Prometheus connectivity and evidence-storage headroom passed. The coordinator failed when creating a fresh Kafka topic; repeated attempts timed out with a broker disconnect. The original shared configuration was restored. Run control remained stopped with no production start.

Kafka controller 1 repeatedly reported `NotControllerException` while identifying itself as the active controller. A single graceful pod restart was requested, retaining all persistent volumes and the other brokers; the pod remained terminating beyond its 30-second grace period. A read-only thread inspection on broker 0 found the metadata Raft I/O thread and scheduler threads in uninterruptible filesystem waits (`folio_wait_bit_common`, `xlog_wait_on_iclog`, `xlog_wait`). Disk usage on broker 1 was approximately 1%, so disk exhaustion was not indicated. Node-health inspection was forbidden to the namespace account. These observations require storage/node investigation; they do not establish the exact underlying storage fault.

Producer and consumer replica targets were returned to zero. No topics, Kafka volumes, or historical evidence were deleted. No force deletion, volume recreation, or whole-cluster restart was attempted.

The six-trial wrapper and continuous-input growth report are implemented. The prior 219-test suite passed; an additional offset-reset regression passed with the four stability tests. Live calibration, resource sizing, metric coverage and matching-start verification remain required. Existing 24 performance-trial results are unchanged.

## Recovery requirements

1. Restore healthy Kafka metadata I/O and verify controller quorum and fresh-topic creation.
2. Restore application pods, verify shared source and stopped configuration, and repeat both calibration rates.
3. Review achieved input, completion, backlog trends and CPU/RSS coverage; record the calibration decision.
4. Run the six planned 23-minute trials with the frozen procedure and preserve their raw evidence.
