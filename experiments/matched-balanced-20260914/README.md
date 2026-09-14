# Matched balanced comparisons

Authorized block: 12 performance trials, not yet results. Reuse the validated
static-membership preparation and original-pod/ownership matching checks from
the later skew comparisons. Each pair uses seed 71 in both arms. Repeat each
workload twice, reversing scale-first/keep-first order. Replication therefore
measures repeat-run variability at this fixed seed, not different-seed variability.

| Workload | Per-producer target | Total target | Warm-up | Evaluated production | Drain | Total |
|---|---:|---:|---:|---:|---:|---:|
| Balanced, lower target rate | 200/s | 600/s | 60 s | 120 s | 120 s | 5 min |
| Balanced | 500/s | 1,500/s | 60 s | 120 s | 120 s | 5 min |
| Balanced | 500/s | 1,500/s | 60 s | 540 s | 120 s | 12 min |

Three producers, 60 partitions, 100-byte payload setting, 2,000 SHA-256 iterations
and no added application sleep. Both arms start with three consumers. Scale arm
requests six at evaluation +60 s (production +120 s). Keep arm stays at three.
Warm-up backlog is retained; warm-up messages are excluded from cohort p99 and
unfinished outcomes. Total production/drain windows: 88 minutes. Preparations,
collection, verification and plotting are additional.

Each of six blocks must pass four empty preparations and both matched performance
trials. Stop on failure; preserve all evidence and restore settings using the
existing block restoration code. No node pinning or targeted redistribution.
Each block captures a current reference; matching is within pairs, not across
all six blocks. Newly added consumers and shared-machine contention remain
uncontrolled. Earlier results are preserved separately.

Run with the project's Python environment:

```bash
python3 experiments/matched-balanced-20260914/run.py --audit-root results/matched-balanced-20260914 --execute
```

Omit --execute to display the commands without cluster actions. An existing audit
root is rejected to prevent overwriting previous evidence. The matched runner
requires clean tracked code; pre-existing untracked duplicate copies named ` 2`
are recorded and excluded only when their original path is tracked. Other
untracked source still blocks execution. Shared code hashes must match every pod.

## Start attempt, 2026-09-14

210 local tests passed; the six-block dry-run listed the expected 12 trials.
Two read-only pod requests succeeded, but both managed shared-state checks
timed out. A bounded follow-up reported an OIDC discovery timeout against
Authentik. No shared configuration/source was changed, no backup/sync step
was reached, and no preparation or performance traffic started. Cluster
access must be reliable before executing this block. Earlier results remain intact.
