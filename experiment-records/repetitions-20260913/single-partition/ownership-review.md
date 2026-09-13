# Why the second single-partition scaling outcome needs caution

The second pair kept the same initial ownership and original pods as the first pair, but scaling created new consumers on different machines. The newly created consumers were not pinned, as requested.

In pair 2, partition 0 moved from consumer 0 on `patternlab.calit2.optiputer.net`, briefly to consumer 4 on `k8s-usra-01.calit2.optiputer.net`, then to consumer 5 on `k8s-chase-ci-04.calit2.optiputer.net`. The last transfer completed at approximately evaluation +83.7 seconds. Consumer 5 then owned partition 0 until shutdown.

| Recorded holder of partition 0 | Mean application-task duration |
|---|---:|
| Pair 1 scaling: original consumer 0 | 1.537 ms |
| Pair 1 scaling: subsequent consumer 4 | 1.275 ms |
| Pair 2 scaling: original consumer 0 | 1.599 ms |
| Pair 2 scaling: brief consumer 4 holder | 1.319 ms |
| Pair 2 scaling: final consumer 5 holder | 3.533 ms |

These means describe recorded partition-0 completion attempts across warm-up, evaluation and drain. They are application-task wall durations, not full consumer service times or independent hardware benchmarks. Changes in machine performance, scheduling and contention are not isolated. No precise causal effect of node choice is established.

The slower recorded task duration on the final owner is consistent with the worse outcome. The scaling overview also shows a lower post-transfer completion rate while incoming traffic remains near the same target. Monitoring gaps still prevent an aggregate backlog comparison for this pair (81.67% coverage); do not bridge those gaps or describe the entire series as continuously measured.

Both treatments had evidence-valid message outcomes and zero recorded duplicate completion attempts. All six failed scale commit events were followed by successes covering their offsets within two seconds. Final commits covered all recorded completed offsets in both treatments. They did not reach all acknowledged partition ends because messages were still unfinished at the cutoff; this is not an unresolved commit failure for already completed work.

The defensible repeated conclusion is that scheduled scaling did not reliably resolve this single-partition workload: it improved the first pair and worsened the second. Consumer count alone does not describe the resulting processing capacity or ownership. These experiments do not establish that a proposed selector, reassignment policy or hot-key splitting algorithm is better than existing research.

[Recorded ownership and task-time calculations](hot-partition-diagnostic.json) · [Pair 2 results](README.md) · [Scaling overview](plots/scale/overview.png)
