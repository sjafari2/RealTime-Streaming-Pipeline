# Controlled concentrated-input repeat

This implementation follows the earlier review design in `review-plan.json`, which remains a dated design record. Targeted reassignment is still unimplemented.

`execute_block.py` runs one seed-71 block in its preselected order: scale 3 to 6, then keep 3. Both treatments use 3 producers, 60 partitions, 1,500 target messages/s total, 80% aimed at partition 0, 100-byte payloads, and 2,000 SHA-256 iterations per message. Production lasts 300 s including 60 s warm-up, then a 120 s drain. The action is scheduled 60 s into evaluation (120 s after production starts). Observation and API time cause a recorded scheduling delay.

The script freezes current producer and original consumer pod identities, nodes, container/image IDs, restart counts, requests and limits before any preparation. The complete reference is partition p -> consumer (p modulo 3). It checks the same reference just before production and again before the scheduled action. Application incarnation and assignment epoch must remain unchanged between those observations. The check validates Kafka's assignment; it does not force an assignment or transfer partitions.

The first successful attempt is preparation-only: fresh empty topics and application readiness are checked, then applications stop without a running state. Every live attempt independently repeats the checks. At most six attempts are allowed per treatment, including preparation-only attempts. The block permits 900 s cumulative preparation: failed/preparation-only attempt time includes cleanup; successful live attempts count time through the production gate. Workload, drain, evidence transfer and analysis for an admitted live trial are separate. Before production the runner checks the remaining preparation budget. Only a different complete initial ownership map may be retried. Changed identity/resources, missing status, exhausted time, or failures after production end the block. All attempts are retained; outcomes never determine retries.

The existing HPA helper saves and pauses competing autoscaling, then restores its original settings. The block backs up and restores the shared configuration and original replica counts, with verification and refusal to overwrite unrelated edits. Consumers 0–2 are retained while extra consumers are removed/added. Equal starting machines do not guarantee constant shared-machine capacity; newly added consumers can land on different machines.

From the final code folder, with working Nautilus authentication and Python dependencies:

```bash
python3 experiments/controlled-followup-20260912/execute_block.py \
  --audit-dir results/controlled-block-NEW_UNIQUE_NAME
```

Choose a new audit directory. The command creates real cluster experiments; there is no implicit automatic rerun after a failed block. It requires a clean committed repository and sufficient free storage. It uses the existing application source, checks the cluster source hashes, and creates fresh topics without deleting earlier results.

For one preparation against an already frozen reference and configured shared YAML:

```bash
bash my-shell/save-run.sh --intervention none --intervention-after 60 \
  --initial-consumers 3 --placement-reference /absolute/path/placement-reference.json \
  --prepare-only --preparation-budget 900
```

The generic command does not manage a block's total attempt limit; use the block script for this comparison. `my-shell/placement_control.py` implements the pure validation rules; `run_experiment.py` invokes them, releases the common timing barrier, journals checks, and uses the existing evidence analyzers. `placement-checks.jsonl` retains the reference hash and observed application/placement records. Each run manifest embeds the reference. The block audit directory contains all attempt IDs, configuration backup, HPA/replica restoration records and run locations. Large evidence remains in `results/RUN_ID` and on the Nautilus PVCs; it is not included in Git.

Report paired run outcomes, conditional completion mean/p99 alongside unfinished share, lag coverage, requested consumer CPU-time, actual owner changes, and scheduling/observation resolution. Audit original consumer placement over each live trial before describing a pair as controlled. This block adds descriptive evidence for one condition; it does not establish exactly-once processing, a 99 ms SLA, targeted reassignment, or selector novelty.
