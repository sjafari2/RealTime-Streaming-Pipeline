# Commit and monitoring transitions: 80/20 scaling run

Reviewed 13 September 2026. Run: `run-20260913-003714`.

**The saved evidence shows that all five failed commit events were followed by successful commits covering their offsets. No unresolved commit failure was found.** This was an offline review; no broker query, deployment or new experiment was performed.

## What happened

Times below are seconds after evaluation started, which was 60 seconds after production started. The scale decision was scheduled at evaluation +60 seconds and recorded at +67.245 seconds. New consumers acquired partitions in two waves:

- Consumers 3 and 4 received partitions at approximately +83.1 seconds.
- Consumer 5 received partitions at approximately +141.25 seconds, triggering a second transfer wave.

These times describe observed callbacks, not exact cross-machine synchronization. The reason consumer 5 joined later was not established by this review.

## Did commits recover?

All errors were asynchronous commit callbacks with `ILLEGAL_GENERATION`. The executed consumer code classifies this as a group-transition error and continues committing currently owned, completed offsets in its normal loop. It does not retry offsets for partitions it has relinquished.

For each offset listed in each failed event, the audit searched subsequent successful commit acknowledgments for the same topic and partition with an offset greater than or equal to the failed offset. It checked all 58 entries, rather than assuming that any later successful commit proved recovery for every partition.

| Consumer | Error time (s) | Offset entries covered later | Longest wait for covering success (s) |
|---|---:|---:|---:|
| 2 | 82.985 | 12/12 | 1.494 |
| 0 | 82.988 | 12/12 | 1.521 |
| 1 | 140.166 | 12/12 | 1.995 |
| 2 | 140.205 | 12/12 | 2.031 |
| 3 | 141.177 | 10/10 | 1.940 |

The logs contain **1,007 successful commit events and five failed events**. For every one of the 60 partitions, the final successful commit offset equals the highest recorded producer-acknowledged offset plus one, and covers the highest recorded completed offset plus one. All six consumers recorded a finish event without a terminal failure.

This is client-recorded commit-acknowledgment evidence. It is not a new inspection of broker state, an exactly-once guarantee or a reason to move the experiment's completion timestamp after commit. The existing evaluation result remains 360,000 completed messages with zero recorded duplicate completion attempts.

## Do the monitoring gaps match the transfers?

Yes, the invalid monitoring intervals occur around the two recorded transfer waves:

| Interval after evaluation start | Excluded time | Recorded reason |
|---|---:|---|
| Approximately 84–98 s | 14 s | Conflicting owner observations, followed by invalid/stale observations |
| Approximately 140–154 s | 14 s | Missing ownership, followed by invalid/stale observations |
| Sampling boundaries | 2 s total | No observation covering the exact boundary |

The two-second sample grid has an approximately 0.00037-second offset from evaluation start. The precise intervals and full invalidity reasons are in the audit JSON. Of 240 evaluation seconds, 210 satisfy the existing adjacent-valid-sample coverage rule: **87.5%**. The audit independently reproduced that total.

A conflicting owner observation in Prometheus does not prove that two consumers processed the same record. Old and new observations can overlap during a transfer. The association with the recorded callbacks supports a transition-related explanation, but the saved exports cannot establish every missing interval's exact internal cause.

Do not fill these intervals with zero, join the curves across them, or remove them from the coverage denominator. This pair remains below the 90% threshold for aggregate backlog comparisons. The completion-outcome result remains usable with its existing limitations.

## Decision

These records do not indicate a required commit-recovery code fix before repeating the experiment. They show that the existing recovery path succeeded in this run. Retain the errors and gaps in the report rather than calling the run error-free.

The proposed next performance step remains the same workload in reverse order: keep-three first, then scale-to-six. It has not started. If aggregate backlog reduction is needed as a primary result, investigate monitoring freshness and the delayed sixth consumer separately before promising adequate coverage; do not loosen the threshold retrospectively.

## Verification

All nine producer/consumer event files match their previously verified PVC-source hashes. The inspected consumer source matches the source hash recorded by all six consumers. All 58 failed offset entries have later covering successes, all 60 final commit offsets match acknowledged partition ends, and all six finish records have no failure. No raw evidence or prior numerical result was changed.

[Reproducible audit script](audit_transitions.py) · [Detailed audit](transition-audit.json) · [Verification](verification.json) · [Original result review](../result-review.md)

To reproduce from the repository root, run:

```bash
python3 experiment-records/8020-comparison-20260913/transitions/audit_transitions.py \
  results/run-20260913-003714 /tmp/8020-transition-audit.json
```
