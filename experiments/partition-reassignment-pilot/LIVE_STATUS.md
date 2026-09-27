# Redistribution pilot: execution status

Updated 27 September 2026. The explicit consumer adapter and scheduled handoff coordinator have been deployed and exercised on Nautilus. Two nonempty-topic technical checks completed all acknowledged messages, and the separate Consumer 2 comparison completed four performance trials. See [results](../../experiment-records/c2-scaling-20260927/README.md) and [preparation evidence](../../experiment-records/c2-scaling-20260927/PREPARATION.md).

The original eight-trial redistribution-only design in this directory has not run. Its candidate 900 msg/s rate and illustrative capacities are not validated settings. The later 700 msg/s ownership calibrations and scale-and-redistribute trials are distinct experiments.

## Implemented and checked

- Explicit starting maps and opt-in application-side assignment, with the default Kafka group mode still available.
- Synchronous completed-prefix commit/readback, evidence flush, and release/acquire/resume verification before ownership moves.
- Authorized new replicas that initially have no partitions, followed by a predefined full target map.
- Run identity, process incarnation, retained offset bounds, source hashes and original-pod placement checks; ambiguous ownership aborts the run.
- Reconciliation of acknowledged identities and completion offsets, including warm-up messages, with duplicate and missing-prefix checks.
- Continuous process CPU/RSS sampling through handoff and an independent observer for requested resources.

All consumers pause for the coordinated handoff, including unchanged owners. The measured action therefore includes global coordination, replica startup and assignment changes. It is a scheduled synthetic-workload experiment, not a Kafka custom group assignor or a claim of production fault tolerance or exactly-once external effects.

## Remaining evaluation

The planner remains offline. It is not connected to live measurements and does not implement the complete adaptive decision policy. Redistribution without scaling still needs its own comparison to separate ownership changes from additional capacity. Capacity calibration, application-state transfer, failure recovery, ordering requirements and intervention costs require further evaluation before extending the mechanism to application workloads.

The local intended suite passed 258 tests after the runtime changes. Nonempty live checks support the successful handoffs actually observed; local fault-path tests do not establish all failure behavior on Kafka. The four performance trials preserved valid unfinished outcomes, and monitoring gaps remain explicitly unavailable.
