# Shared configuration storage replacement — 15 September 2026

The active configuration claim is now `pipeline-config-recovery-ucsd-20260915`, a 5 GiB ReadWriteMany volume using `rook-cephfs-ucsd`. The application path remains `/config/pipeline-configmap.yaml`. Both live producer and consumer StatefulSet templates point to this same claim. Local producer, consumer and optional consumer-application manifests match it; the optional workload was not deployed.

The original `pipeline-configmap-pvc` was retained, not deleted: it contains historical frozen configurations and run manifests that could not be read during the failure. Its contents have not been migrated. Creating a replacement on `rook-cephfs-central` remained pending; the unused test claim and pod were removed. A UCSD replacement provisioned successfully, mounted in a diagnostic pod and accepted file writes.

Restored the versioned `src/pipeline-configmap.yaml` byte-for-byte, verified by reading it back. This is the local template, not a claim to have recovered the latest unreadable live settings. It has ACKS=all and 24-hour retention; experiment-specific rates and timing must be installed by the managed coordinator before any trial. Wrote a stopped run-control record so the consumer supervisor cannot start traffic from an old run. The old run history remains on the retained original volume and existing local evidence archives.

Stopped the three idle producers (their logs showed waiting for manual start), saved both StatefulSet definitions, and changed only their configuration claim references. A fresh consumer then completed its init container and started its main container on the new volume, as recorded by its UID-specific Kubernetes events. It was stopped after this check. Both StatefulSets target zero replicas. Kafka storage, producer-data and consumer-code/evidence PVCs were not deleted or changed. The producer-data volume still uses central storage and needs a separate readiness check before experiments.

Validation: all four changed manifests parsed successfully using kubectl client-side dry-run with schema validation disabled; configuration read-back matched; both live claim references verified; consumer container startup observed. The schema-enabled validation attempt waited for browser authentication and was canceled; schema validation was not completed. No new performance trial ran and the completed comparison count remains 24.

Remaining work: finish the stability wrapper, verify producer data access, synchronize source, calibrate rates/resources, and verify monitoring and controlled startup. This storage recovery does not establish full experiment readiness.

Snapshots, restored-config checksum and UID-specific events:
`/Users/soheila/Documents/ChatGPT/Stream Processing Project/review/config-replacement-20260915/`.
