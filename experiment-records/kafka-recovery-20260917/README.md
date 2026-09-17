# Kafka replacement validation — 17 September 2026

A fresh three-broker cluster was deployed without deleting the original Kafka claims. Storage write/fsync/readback and remount checks passed. The isolated replacement `pip-kafka-recovery-ucsd` passed three-voter quorum inspection, three-partition/three-replica topic creation with all replicas in sync, and an exact three-message producer/consumer check.

An earlier replacement attempt, `pip-kafka-recovery`, was stopped at zero replicas after its third volume failed to mount and remained attached to the previous node. Its full message test failed and is not reported as successful. Those test volumes remain retained. A node hosting one UCSD replacement broker later became NotReady; Kubernetes rescheduled that broker and the subsequent functional test passed.

The original three 300 GiB Kafka claims were not deleted or migrated. The new cluster has separate identity and fresh volumes. Old records and offsets are not part of it. Saved experiment evidence remains separate.

The pipeline bootstrap-service switch and persistent Prometheus configuration change have not been applied. The running replacement alone does not mean the pipeline has switched. No stability trial has started. Deployment and rollback specifications are in `k8s/kafka-recovery-20260917`.

Follow-up checks verified all three original test messages after sequential broker restarts. All three JMX endpoints returned Kafka metrics with no scrape error. Cold scrapes took up to about 21 seconds; the prepared, unapplied monitoring job allows a 25-second timeout and 30-second interval. The earlier NotReady host is now excluded. The existing pipeline service selector was read back and still identifies the original cluster.
