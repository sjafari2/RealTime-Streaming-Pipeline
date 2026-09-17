# Second matched-start comparisons — 13 September 2026

Run the 80/20 pair first, followed by the single-partition concentrated-input pair. Each block uses **keep-three first, then scale-to-six**, reversing its preceding pair. Complete four empty preparations before each pair; stop that block on its first failure, with no retries and a cumulative 1,200-second preparation budget. Preserve every attempt. Check restoration and evidence before starting the next block.

Both workloads retain seed 71, three producers at 500 messages/s each, 60 partitions, 100-byte payload setting, 2,000 SHA-256 iterations, no added sleep, 300 seconds production including 60 seconds warm-up, and 120 seconds drain. Decision scheduled at evaluation +60 seconds. Static membership and full starting-ownership checks stay enabled. Capture a fresh reference for each block; compare it with its preceding reference and disclose any change. Add no machine-placement constraint.

- **80/20:** 80% across selected partitions 5, 10, 25, 33, 35, 39, 43, 45, 51, 55, 58, 59; 20% across the other 48.
- **Single partition:** 80% to partition 0; 20% across the other 59.

The added order option changes local orchestration, not producer/consumer processing or measurement. Keep default scale-first behavior for older commands. Record the actual execution revision and source hashes.

Launch each block separately, only after the preceding block is stopped/restored:

```bash
python3 experiments/controlled-followup-20260912/execute_block.py --static-startup --include-comparison --trial-order keep-first --workload 80-20 --audit-dir 'results/repetitions-NEW_UNIQUE_NAME/8020-restart/block'
python3 experiments/controlled-followup-20260912/execute_block.py --static-startup --include-comparison --trial-order keep-first --workload single-partition --audit-dir 'results/repetitions-NEW_UNIQUE_NAME/single-partition/block'
```

Save per-run outcomes and plots, commit-transition checks, original/restored configuration and placement, PVC/local hashes, and a verified raw-evidence archive. Compare repetitions at the run-pair level, with unfinished outcomes beside conditional latency. Exclude aggregate backlog claims for coverage below 90%; do not repair gaps retrospectively. These are scheduled scaling comparisons, not an adaptive-policy or reassignment evaluation.

Validation before launch: 158 tests passed. The planned four performance trials are not completed results until their records verify.

Operational exception: the initial 80/20 block stopped on a pod-list API failure during empty preparation, with zero performance trials. Restoration, stopped applications and evidence were verified. Preserve [the aborted attempt](../../experiment-records/repetitions-20260913/aborted-8020-preparation/README.md). One fresh block uses `8020-restart/block`; if it fails, stop instead of looping.

Completed: both fresh blocks passed their four empty checks and both trials. All four performance runs, their limitations and the retained aborted preparation are recorded in [the repetition report](../../experiment-records/repetitions-20260913/README.md). Original settings and stopped applications were verified.
