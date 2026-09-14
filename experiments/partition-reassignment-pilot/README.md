# Whole partition redistribution pilot

Status: preparation only. The local planner and handoff evidence contract are tested; there is no live Kafka assignment adapter, deployment or new performance result. `review-plan.json` is not accepted by `save-run.sh` and must not be applied as a ConfigMap.

## First experiments

Use the same 80/20 partition workload in two initial ownership conditions. There are 60 partitions, three producers and three consumers. All consumers initially own 20 partitions. The same 12 hot partition IDs are used in both conditions and across seeds; only the producer sampling sequence changes with seed.

| Condition | Consumer 0 hot partitions | Consumer 1 hot partitions | Consumer 2 hot partitions |
|---|---:|---:|---:|
| Imbalanced assignment | 8 | 4 | 0 |
| Balanced assignment | 4 | 4 | 4 |

The first comparison retains the imbalanced assignment versus enabling targeted redistribution. The second retains the balanced assignment versus enabling the same policy. A healthy balanced condition should produce no move, rather than forcing a useless reassignment. Each condition has two paired repetitions with workload seeds 71 and 72 and reversed action order: eight performance trials in total. Do not pool the two conditions or treat the eight trials as already executed.

Candidate workload: 900 messages/s total (300 per producer), 100-byte payload, 2,000 SHA-256 iterations, no added sleep. Production lasts 600 seconds including 60 seconds warm-up, followed by 120 seconds drain. The decision is 120 seconds after evaluation begins (180 seconds after production starts), leaving 420 seconds of continued input. These values need calibration and user review before freezing. Eight production/drain windows total 96 minutes; preparation, validation, calibration and collection are additional time.

At the candidate input, a hot partition receives 60 messages/s on average and a cold partition 3.75/s. The expected consumer arrival rates are 525/300/75 for the imbalanced map and 300/300/300 for the balanced map. These are configured expectations, not observations or capacity estimates. The illustrative 400 messages/s capacity in review-plan.json is only a test fixture. Do not infer capacity from the inverse of application-task duration or label this fixture as measured.

## Calibration and release gates

1. Confirm the assignment mechanism below. Use that same mechanism in keep and redistribution arms; do not compare manually assigned treatment with an automatically assigned baseline.
2. Implement the live adapter and pass release-before-acquire tests on nonempty topics, including prefetched unfinished records, swaps, failed commits, timeouts, stale commands, process restarts and evidence failures. Empty preparations cannot prove correct offset handoff.
3. Measure sustainable completion capacity using the same work and resource settings on the current consumers, with instrumentation active. Then verify sustained source backlog growth under the imbalanced map and spare capacity at feasible destinations. Check the balanced map separately. Freeze rate and capacity estimates before paired trials; do not tune against performance outcomes.
4. Stop applications, create fresh run topics, freeze configuration and exact ownership maps, and verify every partition has exactly one owner. Record actual nodes and original process identities without adding node pinning. Stop the block on a mismatch; do not keep retrying until a favorable machine appears.
5. Obtain the user's confirmation of the final workload, timing, actions and count before performance trials.

## Assignment mechanism decision

The current consumer calls subscribe() with cooperative-sticky under the classic group protocol. Its assignment callback accepts Kafka's assigned list. The current coordinator implements only scheduled none/scale actions; neither path implements targeted partition reassignment.

Two feasible implementation directions require different claims:

- An explicit-assignment Python pilot can set the starting maps and coordinate whole-partition handoffs. It bypasses normal group assignment. The coordinator must enforce ownership exclusivity and stop on unverified failure; group membership must not be presented as fencing. This tests the redistribution idea under a controlled coordinator, not a custom Kafka group assignor or production fault tolerance.
- A group-managed custom assignment implementation retains broker-coordinated assignment but needs a compatible client/assignor integration. Switching client language or protocol requires fresh calibration and matching it across both arms. It is a larger implementation step.

Do not mix subscribe() and manual assignment to force a destination in the existing cooperative callback. Do not use Kafka broker partition-reassignment commands: they move broker replicas, not consumer ownership. The mechanism choice is pending user input.

Official API reference: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

## What the local code does

`planner.py` validates a complete, fresh, single-topic ownership/rate snapshot and independently dated capacity estimates for equal-cost records. It proposes whole-partition moves and swaps toward spare capacity, using a greedy load objective. The overload trigger is utilization above 1; the default destination target is 0.85. These are provisional pilot rules, not validated research thresholds. It never emits executable instructions. Missing or stale evidence is rejected. Insufficient total capacity and an indivisible overloaded partition are explicit outcomes. A partial search result that cannot reach the target within the changed-partition budget is discarded. Greedy failure does not prove that no global solution exists.

The planner does not yet implement persistence windows, state-transfer costs, arrival forecasting, recovery-time prediction or the complete adaptive policy. Capacity/rate uncertainty can invalidate its load estimate. A live adapter must revalidate identity, ownership, freshness and capacity before any proposed move.

`handoff.py` is an offline evidence-state validator, not a lock service or Kafka client. It checks the intended protocol:

1. Old owners finish or abandon their prefetched work safely, stop processing moved partitions, verify the completed-prefix commit, flush outcome evidence and unassign them.
2. Only after every moved partition has a confirmed release may destinations acquire the exact next offsets while remaining paused. This barrier also supports swaps.
3. Verify complete resulting ownership and unchanged process incarnations, then resume. Record release, acquire, resume and first-completion events.
4. A timeout or ambiguous old-owner failure never authorizes takeover. Abort and preserve evidence. Never advance to consumer position merely because a batch was fetched.

The validator trusts evidence supplied by its future adapter. Unit tests do not prove these facts occurred on Kafka or establish exactly-once application effects. New owners must also check retained low/high offsets and refuse expired or out-of-range progress.

## Measurements and validity

Retain completion p99 and unfinished count/percentage together, useful throughput, partition processing backlog, valid monitoring coverage, requested consumer CPU time and transition timings. Compare both whole evaluation and explicitly secondary before/after intervals. Use the same evaluation/drain horizon in paired arms. Inspect duplicate attempts, missing identities, offset continuity and completion evidence across ownership epochs; output hashes alone are not proof of durable exactly-once effects. No stateful application is claimed.

A successful transfer can still be slower than keeping the assignment. Retain that outcome. Show all paired results and the policy's no-action choices. Existing 24-trial results stay unchanged and are not interchangeable repetitions of this new assignment mechanism.

## Local checks only

From the code folder:

```bash
python3 experiments/partition-reassignment-pilot/build_review.py
python3 -m pytest tests/test_partition_reassignment_pilot.py -q
```

The first command regenerates only the review JSON and prints illustrative candidate decisions. Neither command connects to Nautilus or launches a workload. To inspect a separately prepared snapshot, use `python3 experiments/partition-reassignment-pilot/planner.py PATH_TO_SNAPSHOT.json`.
