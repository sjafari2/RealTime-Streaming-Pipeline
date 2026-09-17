# Isolated Kafka storage recovery

This deployment creates `pip-kafka-recovery-ucsd` separately from the original `pip-kafka` installation. The original three Kafka PVCs are retained. It uses Kafka 4.0.0, three fresh 20 GiB `rook-ceph-block` claims, one CPU/two GiB memory requested per broker, four CPU/four GiB limits and a 512 MiB initial/one GiB maximum JVM heap. JMX sidecar resources remain as in the earlier deployment. These changes require new calibration before performance comparisons.

The recovery was constrained to three UCSD-area hosts after unrestricted placement encountered an RBD attachment failure. One of those hosts subsequently became NotReady; Kubernetes moved its broker. The validated placement uses two hosts, with two brokers sharing one host. This is not a demonstration of tolerance to losing either host. The StatefulSet uses OnDelete updates to avoid unexpectedly rolling healthy brokers during recovery.

## Provisioning a new instance

The recorded deployment already exists. Do not create a new identity for populated volumes. `create_identity.py` uses `kubectl create`, so an existing identity is not overwritten. On a clean installation, create the identity before applying deployment.json. The manifest does not replace the original Kafka StatefulSet, claims or bootstrap service.

## Pipeline switch

The reviewed `bootstrap-cutover.json` changes only the selector of service `pip-kafka` to the recovery instance and tests the original selector first. `bootstrap-rollback.json` provides the reverse patch. Producers and consumers must be stopped for either operation. Recovery of the old cluster is required before rollback could provide a functioning service.

At initial publication, cutover and the Prometheus update are pending explicit approval after automatic approval review blocked those actions. `prometheus-job.json` is the separate three-target scrape job prepared for the existing Prometheus configuration. Verify live status and the recovery record before applying changes; these files are not evidence that cutover happened.

Functional validation includes storage write/fsync/hash verification, remount/hash verification, a three-voter Kafka quorum, a three-partition topic with all three replicas in sync, and an exact three-message produce/consume check. This is readiness evidence, not a stability performance trial. No historical Kafka records or offsets have been migrated.
