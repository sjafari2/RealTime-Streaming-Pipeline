# Scaling from a concentrated Consumer 2 assignment

This scheduled comparison starts with 3 consumers and all 12 hot partitions on Consumer 2. Three producers generate 700 messages/s in total, with 80% across partitions 0–11 of 60. Processing uses 2,000 SHA-256 iterations and no added sleep. Each original consumer initially owns 20 partitions.

The baseline keeps that assignment. The intervention requests six replicas at evaluation +60 seconds, then transfers ownership through a full-consumer release/acquire/resume barrier. New replicas begin with no partitions. After the verified handoff, partition p belongs to consumer p modulo 6: each consumer has two hot and eight cold partitions. Producer routing is unchanged. **This measures added capacity and a predefined redistribution together, not Kafka's automatic assignment algorithm or an adaptive controller.** A redistribution-only comparison is needed to isolate the value of extra replicas.

Each performance trial has 60 seconds warm-up, 300 seconds evaluation and 120 seconds drain: eight minutes total, excluding preparation and evidence collection. The order is keep-three/run 1, scale-six/run 1 (seed 71), scale-six/run 2, keep-three/run 2 (seed 72). Warm-up messages remain in the system but are excluded from the evaluation cohort. Original producer/consumer pod identities, machines, resources and the exact starting ownership map are checked before traffic and before the scheduled action. New replicas' placement is observed, not pinned. An identity mismatch stops the block.

Before performance trials, a separate technical validation uses 20 seconds warm-up, 220 seconds evaluation, 60 seconds drain, and an action at evaluation +20 seconds. It must finish all acknowledged messages, show complete release/acquire barriers and preserve contiguous completion offsets without duplicate or unmatched completions. It is excluded from performance-trial counts. Fault-path tests cover failed commits/readback, evidence failure, stale commands, unapproved joins, original-process restarts and premature ownership claims. These checks do not establish production fault tolerance or exactly-once external effects.

A timeout or ambiguous ownership aborts the managed run. The controller never guesses an offset or takes over from an unverified owner. The global pause and replica startup are part of intervention cost. Monitoring gaps remain unavailable. Completion latency is measured before commit acknowledgment and must be reported with unfinished messages. All raw outcome, monitoring, resource and transition records are retained for analysis.

Inspect the design without contacting the cluster:

```bash
python3 experiments/c2-scaling-20260927/run.py
```

After committing source and syncing the stopped consumer applications, run the validation and four trials:

```bash
python3 experiments/c2-scaling-20260927/run.py --execute
```

The runner pauses competing HPAs, saves configuration/reference records, uses fresh topics, and restores the original configuration and three replicas. It does not delete old topics or evidence. A failed validation blocks the performance trials. Raw data are saved under `results/c2-scaling-<timestamp>` and the individual `results/run-<timestamp>` directories; reviewed summaries belong in `experiment-records/`.
