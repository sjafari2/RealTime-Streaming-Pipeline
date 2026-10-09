# Kafka deployment

This directory contains the Kafka deployment used for the balanced-rate and 700-message/s intervention experiments. It runs Kafka 4.0.0 with three brokers and dedicated storage. The Kubernetes resource prefix `pip-kafka-recovery-ucsd` is retained because it identifies the deployed resources.

| Setting | Per broker |
|---|---|
| Persistent storage | 20 GiB, `rook-ceph-block` |
| Requested resources | 1 CPU, 2 GiB memory |
| Limits | 4 CPUs, 4 GiB memory |
| JVM heap | 512 MiB initial, 1 GiB maximum |

[deployment.json](deployment.json) defines the resources. [create_identity.py](create_identity.py) creates the KRaft identity Secret for a fresh deployment; it uses `kubectl create` and does not overwrite an existing identity. Existing storage must retain its matching identity. [prometheus-job.json](prometheus-job.json) contains the broker JMX scrape configuration.

The experiment clients use the `pip-kafka:9092` bootstrap service. Service routing and monitoring targets must match the deployed brokers before a run. Deployment definitions are not a report of current cluster health.

The cluster was introduced after storage problems in the earlier installation, which used different broker resources. Its original recovery records and rollback patches are preserved on the [archive branch](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008/k8s/kafka-recovery-20260917). Comparisons across those deployments require separate calibration. These experiments do not establish broker fault tolerance.
