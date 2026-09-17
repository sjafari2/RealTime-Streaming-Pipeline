# Next experiment configuration - for review

> Status update, 12 September 2026: the quick balanced configuration below was
> executed as `run-20260911-234758`. See its [saved record](../../experiment-records/run-20260911-234758/validation-report.md).
> The original planning text below is retained as history. Current preparation is
> [pressure calibration](../capacity-calibration/README.md), which has not been run.

Prepared from the current online `main.tex`, Section 4.3, Tables 1 and 2, in
[Proposal_2026_Updated](https://www.overleaf.com/project/6aa3337d078205869d3c5fe5).
Page numbers may change during proposal revisions. No experiment was launched and
the shared `/config/pipeline-configmap.yaml` was not changed for this plan.

## The next useful check

Run a balanced, fixed-six-consumer monitoring validation before deciding the first
scaling workload. It complements the completed skewed run
`run-20260911-214924`. This addresses the monitoring question and prepares the
balanced cases in Table 1. It is not yet evidence that capacity is sufficient (Case A),
that input exceeds capacity (Case B), or that an adaptive controller avoids actions.

I recommend reviewing `quick-balanced.yaml` first. It deliberately keeps the same
short timing as the completed skewed validation, so traffic distribution is the main
configured change. This is a screening run, not one of the proposal's final pilot
repetitions. `proposal-balanced.yaml` is the longer timing option from Table 2; use it
only after the workload, clock measurement, coverage and storage plan are settled.

| Setting | Completed skewed validation | Next quick balanced validation | Proposal timing option |
|---|---|---|---|
| Status | Completed | Proposed, not run | Proposed, not run |
| Producers / consumers | 3 / 6 | 3 / 6 | 3 / 6 |
| Broker/topic/partitions/RF | 3 / 1 / 60 / 1 | Same declarations; verify deployment | Same declarations; verify deployment |
| Rate per producer / total target | 3,000 / 9,000 messages/s | Same, provisional | Same candidate; calibrate before acceptance |
| Input distribution | 80% to 12 hot partitions | Uniform random over 60 partitions | Uniform random over 60 partitions |
| Mitigation | None | None | None; optional reference marker at 300 evaluation seconds |
| Payload / seed | 100 bytes / 1 | Same | Same |
| Synthetic delay / CPU iterations | 0 / 0 | Same | Same provisional task; not representative application calibration |
| Production, including warm-up | 300 seconds | 300 seconds | 1,500 seconds |
| Warm-up / evaluated production | 0 / 300 seconds | 0 / 300 seconds | 300 / 1,200 seconds |
| Pre/post reference periods | Not defined | Not defined | 300 / 900 evaluation seconds |
| Drain / final scrape hold | 60 / 15 seconds | 60 / 15 seconds | 300 / 15 seconds |
| Repetitions now | One observed | One screening run proposed | Proposal prescribes five; not scheduled in this review |

Balanced routing is probabilistic, so per-partition counts will be approximately
equal, not identical. The retained skew keys in the YAML are ignored when
`TRAFFIC_MODE` is `balanced`. The 99 ms completion objective is provisional. The
endpoint remains synthetic processing completion before commit acknowledgement.

## What decides the next step

1. Check distinct acknowledgements against completion, unfinished messages, duplicates,
   evidence drops and exports. Include valid lag coverage and clock uncertainty with
   the results. The previous 87.33% lag coverage and wide clock bounds need improvement
   before strict latency/SLO comparisons are accepted.
2. Check actual admitted rate, assigned partitions, per-partition demand and lag over
   time. Completing all messages by drain does not establish sustainable capacity.
   A brief latency spike alone does not establish sustained overload.
3. Fix a processing task and resource limits for capacity calibration. The current
   zero-delay/zero-extra-CPU workload is a measurement check. Measure capacity rather
   than infer it from the provisional 9,000/s input target.
4. If a balanced workload has sustained backlog and producers/brokers are not the
   limiting stage, prepare a matched Case B pair: fixed six consumers versus scheduled
   scale-up from six to twelve. Twelve is a proposed first step within the table's
   6-24 range, not a demonstrated optimum. The target workload/rate remains pending
   calibration. A single sequential hot partition is not the first scaling-benefit case.

The existing 80/20 skew run is a static-skew screening workload. It is not automatically
Case C: that case also requires observed hot-partition co-location and spare capacity
on other consumers. Targeted reassignment, moving hotspots, cost skew, key splitting
and the adaptive selector are later work; they are not claimed by these configurations.

## Supported run commands after approval and configuration application

The YAML files are complete shared-file configurations, not CLI flags. With applications
stopped, back up the existing shared YAML and copy the reviewed file to
`/config/pipeline-configmap.yaml` on its shared volume. Do not treat a local template
edit or `kubectl apply` as proof that this mounted shared file changed. Verify its
contents and actual StatefulSet replica counts before starting. The runner freezes it
and generates fresh run/topic identifiers.

From the code folder, a complete quick screening run uses the existing entry point:

```bash
bash my-shell/save-run.sh
```

For the longer no-action timeline, after applying `proposal-balanced.yaml`:

```bash
bash my-shell/save-run.sh --intervention none --intervention-after 300 --initial-consumers 6
```

For the later calibrated scale-up counterpart, with identical workload and timing:

```bash
bash my-shell/save-run.sh --intervention scale --intervention-after 300 --initial-consumers 6 --target-consumers 12
```

The action is 300 seconds after evaluation starts, which is 600 seconds after
production starts when warm-up is 300 seconds. Do not use this action time for the
300-second quick configuration: the action would fall outside its evaluation window.
Adding consumers requires the already documented supervisor setup and verified source
on new replicas. Ordinary scaling/rebalancing is not targeted reassignment.

When repetition count and storage capacity are established, the same flags are supported by
`bash my-shell/run_pipeline.sh --repetitions 5`. Paired configurations need separate
batches with the same reviewed workload, timing and starting count. Do not combine
the old short run with the longer runs as identical repetitions. The final scale-up
count stays in place; `--initial-consumers 6` restores the baseline before each run.

## Duration, storage and evaluation

| Option | Scheduled production + drain + final scrape hold | Estimated evidence at the candidate rate |
|---|---|---|
| One quick validation | 6 min 15 sec, plus preparation/transfer/analysis | About 2.2 GB |
| One proposal-duration run | 30 min 15 sec, plus preparation/transfer/analysis | About 10.9 GB |
| Five proposal-duration runs | At least 2 h 31 min 15 sec, plus overhead | About 54.5 GB |
| Five no-action + five scale-up runs | At least 5 h 2 min 30 sec, plus overhead | About 109 GB; scaling changes exact size |

These are volume projections from the 2.18 GB, five-minute measured run; they are not
reservations or guaranteed compression sizes. Warm-up records also occupy storage.
Each full-rate 25-minute run attempts about 13.5 million messages. Analysis needs
additional temporary space and can take longer than production. The roughly 14 GiB
free locally during planning is insufficient headroom for a longer batch. Even 70 GB
of external free space needs a known destination and separate local analysis space;
it is not enough for ten uncompressed runs at this projected size.

The primary outputs remain exact whole-run completion distributions with unfinished
and deadline outcomes, throughput, lag with coverage, actual assignments, and declared
consumer resource-time. Action runs additionally record action/rebalance/readiness
and observable recovery. Rolling 30-second Grafana p99 is a monitoring estimate and
is reported separately from exact cohort p99. Keep one result per repetition and its
configuration/code identity before any aggregate comparison.

No proposal definitions were changed for this review. Table 2's longer timing and
five-repetition plan remain intact; the short option is an explicitly proposed
measurement-validation step.
