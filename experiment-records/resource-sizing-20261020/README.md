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

## Cluster changes and remaining blockers

- The managed run was already stopped. Its last run ID was `run-20260930-012528`.
- `consumer-sts` and `producer-sts` were scaled from three desired replicas to zero. Three consumers and two reachable producers terminated normally.
- The old `pip-kafka-controller` cluster was scaled from three to zero. The active `pip-kafka` service selects the replacement `pip-kafka-recovery-ucsd` brokers; the old cluster returned no consumer groups before shutdown. All three old broker pods terminated.
- All 17 PVC identities and backing volume names were verified unchanged. No PVC, topic or evidence file was deleted.
- The three active replacement brokers, Prometheus and Grafana remain running. Broker resource declarations and JVM heap are unchanged; Python process measurements are not a basis for sizing Kafka.
- `producer-sts-1` remains Terminating on an unreachable node since October 5. It was not force-deleted. A safe node/pod recovery check is required before restoring three producers.
- The revised client resource templates are saved in the repository. Server-side dry runs remain rejected by the Nautilus utilization admission webhook. They have **not yet been deployed**.
- Hard producer–consumer separation remains undeployed. It must be applied and verified, alongside the smaller resources, before the next campaign. Existing live locality exclusions must be preserved when patching.

The cleanup releases unused reservations while retaining evidence. Administrator help is needed if the stale utilization record continues to prevent corrective updates, and to resolve the unreachable-node producer. No performance result is inferred from these maintenance checks.
