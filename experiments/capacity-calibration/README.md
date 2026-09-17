# Pressure calibration — prepared, not executed

The next measurement is intended to create sustained processing backlog. The two
completed pilots at a target of 9,000 messages/s had brief backlog peaks; they do
not establish the six-consumer capacity limit. This preparation does not change
the live shared configuration or start a workload.

## Workload and sequence

Keep three producers, six consumers, 60 partitions, balanced routing, a 100-byte
payload and seed 1. Keep added processing delay and CPU iterations at zero and use
no mitigation. This measures the capacity of the current pipeline, including its
instrumentation. A representative application task will need its own calibration.

| Stage | Target per producer | Total target | Expected messages during 180 s at target |
|---|---:|---:|---:|
| First prepared stage | 4,000/s | 12,000/s | 2,160,000 |
| Consider only if the first remains stable | 5,000/s | 15,000/s | 2,700,000 |
| Consider only after reviewing the second | 6,000/s | 18,000/s | 3,240,000 |

Run one stage, inspect its evidence, then choose the next step. Stop increasing the
rate when useful overload is observed. These rates are candidates, not measured
capacity or a guarantee of overload. If all three remain stable, review the actual
bottleneck and resource/storage headroom before choosing a higher rate. Do not run
these rates as an unattended sweep.

Each stage has 180 seconds of production: 60 seconds of warm-up followed by 120
seconds of evaluation. Allow 120 seconds of drain and 15 seconds for final scrapes.
The scheduled total is 5 minutes 15 seconds, plus preparation, transfer and analysis.
Only change the rate between stopped runs; the runner freezes each configuration.
This short calibration does not replace the proposal's longer comparison and five
repetitions. Later comparisons must use matched timing and processing settings.

## What would count as useful pressure?

Processing backlog is the sum of `high offset - sequential completion frontier`
over the run's partitions. The frontier is the next offset after completed work.
This is separate from returned-record position lag and committed-offset lag.
Require complete, valid ownership and offset observations before summing; a partial
sum can hide overload on the missing partitions.

For intuition only: if brokers admit 12,000 messages/s but consumers complete
10,000/s, outstanding work grows by about 2,000 messages/s, or 120,000 in a minute.
This balance assumes the same traffic/time interval without duplicate, failed or
missing records. It is not a measurement from these experiments. Setting a target
of 12,000/s does not prove that brokers admitted it.

The provisional manual screening rule in `calibration-plan.json` asks for two
consecutive 60-second evaluation windows, each with at least 95% valid observation
coverage and at least 1,500 offsets of growth between the median backlog in its
first and last 10 seconds. Review the full trace for persistent growth rather than
accepting two endpoint comparisons alone. Also require:

- Positive admitted-arrival minus completion rate over the same windows; inspect
  time series and post-run evidence. Do not compare arrivals from one interval with
  completions from another or divide a cohort count by a rolling window.
- Backlog across multiple partitions, with recorded assignments. A single hot
  partition could indicate a different problem from broad consumer overload.
- Complete evidence and investigation of send failures, duplicates or missing
  observations. Prometheus completion counters count attempts; distinct identities
  come from the post-run evidence.
- Measurements supporting a consumer-stage bottleneck. Check actual producer
  acknowledgements, producer buffering/errors, consumer process CPU, container
  throttling/resources and broker/node/storage/network behavior where available.
  CPU alone is insufficient. If attribution is uncertain, call it pipeline overload.

The 95% and 1,500-offset criteria are exploratory choices, not established research
thresholds or an automatic classifier. An admitted rate below 95% of target is a
tracking warning to investigate; consumers may still be overloaded at that rate.
Fully draining later does not erase overload during production. Report unfinished
messages and deadline outcomes alongside completed-message latency.

## Checks and stopping conditions

Read [measurement-readiness.md](measurement-readiness.md) before execution. Verify
source/configuration hashes, three/six actual replicas, all 60 assignments, evidence
health, clocks, monitoring and storage at each stage. The live diagnostic report is
a point-in-time check; repeat the clock check at future run boundaries.

The additional limits are manual operator criteria, not guards implemented by the
runner: end production if valid total processing backlog exceeds 600,000 offsets
for 10 seconds, monitoring is invalid for 10 seconds during evaluation, evidence
drops/writer failures occur, repeated send/processing errors occur, or pods restart,
OOM or approach storage exhaustion. Startup has an expected initial offset gap;
require valid monitoring before evaluation starts. The backlog/storage/error limits
apply during warm-up too. Mark an early stop as interrupted/threshold-limited and
retain the evidence; do not call it a completed formal trial. A threshold hit during
warm-up can help bracket capacity but cannot satisfy the two-window screening rule.

The runner enforces scheduled production and drain. While a complete run is active,
one Ctrl+C in its terminal invokes managed stop, bounded drain, collection and
analysis. Allow that cleanup to finish. If the original coordinator is inaccessible,
the existing `save-run.sh stop` command ends production and allows drain; it does
not itself export/analyze the evidence. Record any external early stop explicitly.

Approximate raw evidence projections, based on the balanced pilot's 2.27 GB for
2.70 million messages, are 1.82, 2.27 and 2.72 GB for these stages. Warm-up records
occupy storage too. Before each stage, require free local space of at least four
times its projected raw bytes plus 8 GiB reserve, at least 2 GiB free on the producer
PVC and 5 GiB on the consumer PVC. These are provisional working-space allowances,
not reservations. Recheck accumulated usage between stages; analysis can take much
longer than production. Preserve the old raw data and topics.

## Files and existing commands

- `balanced-pressure.yaml`: complete first-stage shared-file configuration.
- `calibration-plan.json`: candidate stages, provisional criteria and manual limits.
- `measurement-readiness.md` and `diagnostics/`: findings from the completed pilots
  and read-only checks on 12 September 2026.
- `../../python-scripts/check_cluster_clocks.py`: read-only checks in existing pods.

From the repository root, this command only reads clock status:

```bash
python3 python-scripts/check_cluster_clocks.py --context nautilus \
  --namespace kafkastreamingdata --output results/clock-before-pressure.json
```

For a later authorized run, first stop the applications, back up the current shared
YAML, copy the reviewed file to `/config/pipeline-configmap.yaml` on the shared
volume, and read it back from both roles. A local template edit or `kubectl apply`
does not update this writable shared file. Confirm its hash and the actual replicas.
Then the existing complete-run command is:

```bash
bash my-shell/save-run.sh
```

It saves results under `results/RUN_ID/`, including frozen configuration, manifests,
producer/consumer evidence, Prometheus export and outcome/lag/execution summaries.
Retain run-scoped plots and their data/queries using the completed pilot's plotting
workflow. Plot export and the extra threshold checks are not automatic features of
this YAML. Later stage rate changes must occur between runs and be saved with the
new run's configuration/code identity before interpreting the results.

The full plan is prepared for review. No new workload, scaling action or batch was
launched as part of this preparation.
