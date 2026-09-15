# Sustained-input stability experiment

Status: no new trial started. The shared configuration storage has been replaced
with a tested UCSD volume; both live workload templates use it. A fresh consumer
completed initialization and started, then was stopped. The original claim remains
intact for historical recovery. Producer data access, the execution wrapper,
calibration, source synchronization and live metric/resource checks are pending.
See the [storage replacement record](../../experiment-records/config-storage-replacement-20260915/README.md).

## Six planned trials

Each trial lasts 23 minutes: 1 minute warm-up, 20 minutes evaluation with production
continuing, and 2 minutes drain. EXP_DURATION_SEC=1260 includes warm-up.

| Condition | Target per producer | Aggregate target | Consumers |
|---|---:|---:|---|
| Lower balanced input | 200/s | 600/s | Keep 3 |
| Balanced pressure | 500/s | 1500/s | Keep 3 |
| Balanced scaling | 500/s | 1500/s | Start 3; scale to 6 at evaluation +300 s |

Repeat each condition twice at workload seed 71; these are repetitions at a fixed
seed, not two different seeds. Reverse pressure treatment order in run 2.
Keep the same 60 partitions, 100-byte payload and 2000 SHA-256 iterations with no
added sleep. Warm-up messages are excluded from cohort latency/unfinished metrics,
but their backlog is retained. Each trial uses a fresh run-specific topic.

The JSON intervention files are incomplete with respect to live placement:
a matching reference must be captured and checked using the existing empty-start
protocol before the pressure pair is released. Do not directly apply these YAMLs
or bypass the frozen-run coordinator. The existing matched-balanced runner describes
an older 12-trial campaign and must not be used to launch this six-trial design.
A stability-specific execution wrapper remains pending the blocked preflight.

## Before production

Verify producer data access and remote execution; verify shared config
and evidence storage. Verify actual resource usage and size requests consistently
for both arms. Preserve the paused HPA and check its minimum remains at or below the
three-consumer baseline. Restore application pods only when preparation can proceed.

Run short capacity calibration at both targets. Record achieved acknowledgments,
completion rate and processing-backlog trend. If the rates no longer represent a
sustainable lower load and a pressure load, revise and record the design before
starting the comparison; do not silently relabel rates after seeing results.

Verify complete per-process CPU/RSS monitoring and partition ownership/freshness.
Verify clock diagnostics, evidence headroom and matching source/package hashes.
Capture then verify starting ownership and original consumer placement within each
pressure pair using empty preparations. Exclude failed preparations from trial
counts, retain unfavorable valid trials, and record all failures.

## Stability assessment

Separate continuous-input behavior from drain. Plot acknowledged input rate,
distinct completion throughput, total processing backlog, partition backlog and
process CPU/RSS over time; mark warm-up, intervention and production end.

Report slopes and covered time for the final ten minutes of evaluation and
successive five-minute windows. Report ownership/freshness gaps and CPU/memory
coverage. A growing backlog indicates insufficient effective capacity at that
achieved load. A decreasing backlog indicates catch-up. Approximately flat backlog
supports bounded behavior only over the observed interval; it is not an infinite-
horizon guarantee. Do not invent an automatic pass threshold after seeing results.
Recovery to 1500 offsets held for 20 seconds is a separate declared metric, not a
proof of stability. Retain cohort p99 alongside unfinished outcomes and deadline
outcomes. Draining alone does not demonstrate stability with continuing input.

If the last window is still evolving, report the result as inconclusive and plan
a longer repeat with recorded timing; do not change frozen boundaries mid-run.
The 80/20 follow-up comes after this balanced stage and its review.

## Outputs and limits

Use the existing full message evidence and Prometheus exports under `results/`,
with small reviewed summaries and hashes under `experiment-records/`. Preserve
raw evidence separately from Git. `campaign.json` is a plan/status file, not results.
Read `METRIC_COVERAGE.md` before reporting full metric coverage.
