# Bounded preliminary campaign, 12 September 2026

This campaign addresses simple research questions 1 and 2: valid partition-level
measurements and the outcome/cost of scheduled consumer scale-out. The work budget
starts at 08:53 UTC and ends at 20:53 UTC. This is a short preliminary campaign,
not the proposal's full Table 1, 25-minute, five-repetition study.

## Current protocol and evidence

The eight core trials in `comparison-protocol-v4.json` are complete. Reviewed paired
reports and plots are under `experiment-records/campaign-20260912/comparisons/`
(`sustained/` and `short/`). Earlier protocol revisions and technical attempts are
retained as history; they are not instructions to repeat excluded attempts.

The dedicated one-partition calibration `run-20260912-083417` passed the criteria
in `single-partition-capacity-plan.json`. Its measured qualification is saved in
`experiment-records/campaign-20260912/single-partition-calibration/qualification.json`.
`isolated-partition-protocol-v2.json` adds that evidence before the four concentrated
trials, preserving their original configurations, seeds, action order and 300 s
production duration. `low-input-protocol.json` defines a separate lower-input
comparison. A prepared protocol is not evidence that its trials have completed;
use the saved run records and final campaign inventory for actual status.

The core and dedicated calibration use 2,000 SHA-256 iterations per message with
no added sleep. Qualified balanced input is 1,500 messages/s across three producers;
the dedicated partition calibration uses 1,200/s. These observations do not define
a universal consumer capacity. The latest relevant full suite passed 96 tests.
The current freshness rule uses local monotonic observation age plus original
Prometheus sample age; exports query a 2 s grid over actual 5 s application scrapes.

Routine future runs use `my-shell/save-run.sh` and `my-shell/run_pipeline.sh` as
explained in `docs/runtime-and-data-flow.md`. Dated campaign orchestration records
preserve the exact guarded sequence and should not be mistaken for another general
runtime entry point. Complete commands own and restore their temporary HPA pause;
this campaign's earlier explicit pause remains under campaign restoration control.

## Preparation history

The following notes retain the order of calibration and infrastructure findings.
Their future-tense statements describe decisions at that point in the campaign,
not pending instructions for the current runtime.

First calibrate real backlog with the committed 180-second balanced pressure
configuration (12,000 target messages/s total, no artificial application work).
Inspect the trial before selecting another. If rate alone does not create an
informative consumer limit, explicitly calibrate the existing SHA-256 task at a
lower message rate. Keep the task and generated workload fixed within comparisons.

Then use matched no-action and scheduled scale-out trials. Freeze timing, workload,
replicas and seed before each pair; preserve both results regardless of outcome.
Repeat valid pairs if time and storage allow. Add a concentrated-partition case only
when its measured input exceeds the owner's processing capacity. Ordinary skew
alone does not establish an indivisible-partition limit. Reassignment, splitting
and a new automatic controller are outside this campaign.

Save admitted/completed rates, partition ownership, completion backlog, completion
latency plus unfinished records, monitoring coverage, action/readiness/first-useful
completion events, placement and resource-time. Preserve per-message evidence on
Nautilus and locally; keep only small summaries/configurations/checksums/plots in
Git. Current producer PVC and local free space constrain trial size; check them
before each trial. Use the calibration plan's conservative storage and stop limits.
Reserve time to inspect plots and reconcile the preliminary-results proposal text.

The observed 99 ms threshold is provisional. Point-in-time kernel clock reports
are not an independently established cross-node error bound. These pilots document
research progress and limitations; they cannot establish first-ever novelty, or superiority over all prior systems.

## Monitoring preparation

The live broker JMX endpoint took 11.79 seconds for an idle request, exceeding its
10-second scrape interval. The revised job discovers each broker separately and
uses a 30-second interval with a 25-second timeout. Application pod discovery also
includes consumers added during scaling. The deployed Prometheus 2.52 promtool
accepted the proposed configuration and its existing alert rules. Deployment and
post-deployment health are recorded in the campaign audit before workload begins.
No broker restart or data deletion is required.

Configuration semantics: https://prometheus.io/docs/prometheus/latest/configuration/configuration/

## Live preparation outcome and next calibration

Nautilus rejected the Prometheus Deployment change because account resource
utilization was low. The original ConfigMap and one Prometheus replica were
restored using the existing data PVC. The new discovery configuration is committed
but **not deployed**. Existing six-consumer static targets remain at a 5-second
scrape interval; broker scrapes remain incomplete. Read-only container metrics are
saved separately. The prepared RBAC grants were not activated by Prometheus.

The first six-consumer calibration is run-20260912-031020. A subsequent calibration
will reduce the initial consumer count to three while preserving its rate, timing,
processing task and workload seed. Nautilus accepted a server dry run of the
standard scale request. A 3-to-6 comparison would remain within the existing
monitoring targets and original replica allocation. Actual scaling outcomes and
startup/placement still need live validation. Calibration runs will be reported
separately from the later fixed comparison protocol.

Completed local event logs may be compressed after collection and validation:

```bash
python3 python-scripts/compress_evidence.py results/RUN_ID
```

Replace RUN_ID with the completed run's identifier. This writes gzip files, verifies
that decompression exactly reproduces each original SHA-256, records both hashes
and byte counts, then removes only the redundant uncompressed local representation.
The original Nautilus evidence is unchanged. The outcome, partition and execution
analyses read either format and reject duplicate plain/compressed representations.
Keep `compressed-evidence.json` with the run. This is lossless storage, not sampling.
The helper refuses incomplete/failed managed runs and shares the runner's local
lock; do not manually modify evidence during collection or compression.

The analysis uses a 64 MiB SQLite cache for each temporary identity database. All
64 checks passed. On the same synthetic 120,000-record workload, the old and new
summaries were identical (14.24 versus 9.38 seconds on this Mac). This small check
is not a full-run speed guarantee or an experimental performance result.

## Replica-control correction

The existing `consumer-hpa` restored six replicas after the attempted three-consumer
scale-down. Run `run-20260912-042303` was interrupted and excluded. The original HPA
settings are backed up; controlled trials require both scaling directions disabled
and a minimum that permits the chosen initial count. The runner now rejects an
active/conflicting HPA and checks actual producer/consumer counts before creating a
topic and again after readiness. It records the paused HPA settings in the manifest.
All 68 local checks passed, including the six-replica-floor regression. Restore the
saved HPA configuration when the controlled campaign ends.

HPA behavior reference: https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/
