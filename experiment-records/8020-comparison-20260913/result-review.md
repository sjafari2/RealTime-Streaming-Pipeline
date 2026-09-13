# Review of the 80/20 comparison

Reviewed 13 September 2026 against the saved summary, paired calculations and both overview plots. This is 80% of traffic spread across 12 of 60 partitions. Scale ran first, followed by keep-three; each admitted 360,000 evaluation messages.

| Outcome | Keep three | Scale to six |
|---|---:|---:|
| Completed by drain cutoff | 358,085 | 360,000 |
| Unfinished | 1,915 (0.53%) | 0 |
| Mean completion latency, completed cohort | 41.76 s | 10.79 s |
| Completion p99, completed cohort | 121.15 s | 49.37 s |
| Requested CPU over the common horizon | 36.00 core-min | 65.16 core-min |

## What the evidence supports

There was real pressure with three consumers. The baseline overview shows approximately 1,500 acknowledged attempts/s against approximately 1,240 completed attempts/s, with backlog growing throughout production. These rolling monitoring rates are distinct from evaluation-cohort outcomes.

Scaling improved the recorded completion outcomes in this pair. Conditional p99 fell by 59.25%, and 1,915 additional evaluation messages finished before the common cutoff. Requested CPU increased by 81.01%; this measures declared resources over time, not actual CPU consumption or a financial bill.

The baseline still finished 99.47% of evaluation messages because the experiment allowed two minutes to drain after input stopped. That high completion percentage does not mean it kept up during production. Neither run confirmed the configured backlog recovery threshold before production ended. Scaling finishing everything by the drain cutoff must not be described as confirmed recovery under continued input.

The observed benefit is consistent with adding consumers helping this workload distributed across multiple busy partitions. It does not establish that scaling solves every skew pattern, that it is the most efficient mitigation, or that a new adaptive policy works. Reassignment and key splitting were not tested. The earlier single-partition stress experiment is a different workload; cross-block differences are descriptive, not a controlled estimate of the effect of skew shape.

## Reliability and limits

Both message-outcome records are evidence-valid, with no pair-compatibility failures and zero recorded duplicate completion attempts. The scale run contains five failed commit-result events, all marked as group transitions; the baseline contains none. Completion is measured before commit acknowledgment. These records therefore support processing-completion claims, not an assertion of error-free commits or exactly-once behavior.

Scaling lag coverage is 87.5%, below the existing 90% screen. Retain the plotted gaps and exclude aggregate backlog reduction claims for this pair. Valid snapshots are useful diagnostics, but missing transition intervals cannot be filled with zero. Resource coverage is 100% for both runs.

This is one pair with fixed trial order. Initial ownership and original pod placement matched, but shared-machine contention and placement of newly added consumers remain uncontrolled. Conditional p99 uses different completed populations; always show unfinished counts beside it. The provisional 99 ms deadline and unverified cross-node clock precision do not support an SLA claim.

## Recommended next experiment — not started

The [transition audit](transitions/transition-review.md) is complete: all failed offset entries received later covering commit acknowledgments within 2.04 seconds; final commits matched acknowledged ends for all 60 partitions. Monitoring gaps align with two transfer waves. No commit-recovery fix is indicated by this run, and the 87.5% backlog-coverage limitation remains. Preserve this completed pair unchanged.

Then repeat the same 80/20 configuration and seed with the order reversed: keep-three first, scale second. Retain 1,500 messages/s, 2,000 SHA-256 iterations, the same timing and starting-ownership checks. This gives a repetition without simultaneously changing traffic rate or the hot set. Verify the initial reference afresh and record any difference from this block; do not silently pool blocks with different starting conditions. Continue without adding machine-placement constraints.

If the benefit repeats, test a separately declared second seed as a robustness check. A different seed can change which partitions are hot, so label that as another workload realization rather than an exact repeat. Review results per pair before calculating a cross-run summary.

## Verification and sources

The review recalculated the p99 and requested-CPU percentage differences, checked both overview plots, checked outcome validity and commit-error counts, and verified all 89 existing artifact hashes without mismatches. No runtime code changed or live experiment was launched for this review.

[Original report](README.md) · [Exact summary](summary.json) · [Paired calculations](comparison/comparison-summary.json) · [Keep-three plot](plots/none/overview.png) · [Scaling plot](plots/scale/overview.png)
