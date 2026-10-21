# Client resource sizing — October 20, 2026

This is a deployment-maintenance record, not a performance trial. No new comparative trials have started.

The next 700 aggregate-message/s calibration uses smaller client allocations:

| Container | Previous CPU request / limit | Revised CPU request / limit | Previous RAM request / limit | Revised RAM request / limit |
|---|---|---|---|---|
| Producer | 2 / 4 cores | 0.25 / 0.30 cores | 1 / 2 GiB | 128 / 152 MiB |
| Consumer | 2 / 4 cores | 1 / 1.2 cores | 1 / 2 GiB | 512 / 600 MiB |
| Bootstrap init, each role | 0.1 / 0.1 cores | 0.1 / 0.12 cores | 1 / 1 GiB, inherited | 128 / 152 MiB |

The init-container declaration matters: a pod's effective request includes the largest regular init-container request. Leaving the namespace's 1 GiB default would defeat the smaller application memory request.

## Evidence and limits

The twelve September 29–30 intervention trials provide the following maximum sampled process values:

| Role | CPU, cores | Resident memory, MiB |
|---|---:|---:|
| Producer | 0.250 | 32.94 |
| Consumer | 1.055 | 275.71 |

[sizing-evidence.json](sizing-evidence.json) identifies source hashes, peak runs and processes. CPU percent is divided by 100 to obtain cores; memory bytes are divided by 1,048,576. Observed process means retain their original coverage. These are process measurements, not a guarantee of container peaks or memory bounds. The proposed limits require fresh calibration, including achieved input, processing capacity, container memory, CPU throttling and restarts. The existing 24-trial results and their resource-cost calculations are unchanged.

## Initial cleanup and blockers

- The managed run was already stopped. Its last run ID was `run-20260930-012528`.
- `consumer-sts` and `producer-sts` were scaled from three desired replicas to zero. Three consumers and two reachable producers terminated normally.
- The old `pip-kafka-controller` cluster was scaled from three to zero. The active `pip-kafka` service selects the replacement `pip-kafka-recovery-ucsd` brokers; the old cluster returned no consumer groups before shutdown. All three old broker pods terminated.
- All 17 PVC identities and backing volume names were verified unchanged. No PVC, topic or evidence file was deleted.
- The three active replacement brokers, Prometheus and Grafana remain running. Broker resource declarations and JVM heap are unchanged; Python process measurements are not a basis for sizing Kafka.
- `producer-sts-1` remains Terminating on an unreachable node since October 5. It was not force-deleted. A safe node/pod recovery check is required before restoring three producers.
- The revised client resource templates were saved in the repository. At the initial cleanup, server-side dry runs were rejected by the Nautilus utilization admission webhook and the templates were not deployed.
- Hard producer–consumer separation was also undeployed at the initial cleanup. The existing live locality exclusions were retained for the later update.

The initial cleanup released unused reservations while retaining evidence. No performance result is inferred from these maintenance checks.


## Corrected templates applied

On October 20, both server-side validations passed and the corrected client templates were applied. The consumer StatefulSet retained its existing UCSD locality and excluded-node rules. The producer StatefulSet was absent at the recheck and was recreated from the reviewed manifest with zero desired replicas, preserving its existing PVC references. Both templates now require producer and consumer pods to use separate nodes within this namespace. Broker separation remains a preference.

[Deployment verification](deployment-check.json) records the accepted resources, init-container settings, affinity rules, and zero desired replicas for both clients. All 17 PVC identities and backing-volume names were unchanged. No experiment was started, and these settings still require workload calibration and checks of actual placement, achieved rate, throttling, memory and restarts.

The old `producer-sts-1` pod remains Terminating on `k8s-gpu-01.calit2.optiputer.net`, with a deletion timestamp of October 5. It was not force-deleted. The current account cannot inspect node health at cluster scope, so administrator confirmation that the old process has stopped, or safe node recovery, is needed before reusing that producer identity. Removing the utilization block does not establish that this separate node issue is resolved.
