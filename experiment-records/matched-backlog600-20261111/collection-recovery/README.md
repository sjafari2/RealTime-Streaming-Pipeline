# Recovered final collection

Run `run-20261111-104120` completed the larger starting-backlog Keep 3 protocol. Kubernetes access failed during final snapshot collection and restoration. Access later recovered, and all producer and consumer processes reported finished. The cause of the access interruption remains unconfirmed.

Original evidence was collected and analyzed with frozen execution revision `8321430676485ac349bbed0328fde8f0f4635b1d`. Identity checks, input-rate checks and all predefined late pressure windows passed. No timing boundary, workload, measurement definition or admission threshold changed. The valid run was retained rather than repeated.

At the common action reference, smaller and larger controls had 14,525 and 43,098 backlog offsets. Both admitted 600 messages/s during evaluation. The three producers had matching evaluation partition sequences across the two preparations. This does not isolate backlog size from message age.

All final evidence files were verified against the retained source. The local backup is on the same device and is not an independent off-device backup. Two resource-journal observations were appended during restoration after the initial backup and outside the measurement cutoff. The final complete backup is under `backup/matched-backlog600-20261111/collection-recovery/run-20261111-104120`, and the initial backup remains preserved.

Original configuration, consumer template, update strategy and three replicas were restored before resuming the remaining six mitigation trials. Two of eight main trials are accepted at this checkpoint.
