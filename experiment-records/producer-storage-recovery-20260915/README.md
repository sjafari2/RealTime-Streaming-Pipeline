# Producer recovery and validation — 15 September 2026

A fresh producer failed to mount `producer-data` (`rook-cephfs-central`): DeadlineExceeded, then an existing target-path operation. Retained that original PVC and all its historical evidence. Created `producer-data-ucsd-20260915`, 10 GiB ReadWriteMany on UCSD, and changed the producer StatefulSet to use it. The mounted path remains `/app/producer-data`. Historical files were not copied from the inaccessible old volume.

Removed only already-terminated old producer records, and later the failed-mount pod whose init and main containers had never started, with UID-preconditioned deletion. Saved the original resource definitions under the local recovery directory. A fresh producer on the new volume became Running and accepted remote commands; its available space was 10 GiB.

Both producer and consumer imported confluent_kafka, prometheus_client, psutil and yaml successfully. Both report Python Kafka client 2.12.2 and librdkafka 2.12.1. Consumer configuration read-back matches the restored template; run-control remains stopped. Consumer evidence volume reported approximately 289 GiB available. `http://prometheus-svc:9090/-/ready` responded ready from the consumer. This does not yet verify every required scrape series.

Synchronized all 11 managed source files across the producer and consumer volumes using my-shell/sync_code.py. Every copied file's SHA-256 matched its local source. Synchronization verified one live pod per role; additional replicas require their own preflight checks. The final successful invocation used Python 3.12 from the workspace directory. Earlier repository-directory invocations timed out at kubectl; the difference is not yet explained. Do not assume repeated API reliability until calibration passes.

Local tests: 214 passed under Python 3.12. An initial Python 3.8 attempt failed four tests because the analyzers use dictionary union, which requires Python 3.9 or newer. No code was changed to accommodate that obsolete runtime. The validated local runtime is `/tmp/pipeline-runner-20260915/bin/python` (temporary environment).

No new performance trials or calibration runs started. Remaining: stability execution wrapper, sustained-window/aggregate reporting, resource sizing and rate calibration, monitoring coverage, and matching-start preparations. The six-trial plan remains pending; this record is infrastructure validation, not new research results.

Saved live resource snapshots:
`/Users/soheila/Documents/ChatGPT/Stream Processing Project/review/producer-recovery-20260915/`.
