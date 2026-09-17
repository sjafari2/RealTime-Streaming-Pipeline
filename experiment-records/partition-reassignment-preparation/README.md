# Partition redistribution preparation

This record documents preparation for the first whole-partition redistribution experiments. The review plan, offline planner and handoff evidence contract are in [the pilot folder](../../experiments/partition-reassignment-pilot/README.md). This is preparation only, not a live Kafka implementation or a performance result.

188 local tests passed, including 30 new tests. They cover balanced no-action, uneven capacities, an indivisible hot partition, group overload, moves and swaps, stale/incomplete observations, resume offsets, release/acquire barriers, restarts and timeouts. The single warning concerns inability to write pytest cache under the filesystem sandbox; all tests completed. Tracked existing tests were selected explicitly so unrelated untracked duplicate files were not collected.

The mechanism choice is pending user input: explicit assignments controlled by Python in both arms, or a group-managed custom assignor. The existing cooperative-sticky consumer and save-run workflow have not changed. No Nautilus deployment, nonempty-topic transfer validation, calibration or performance trial has occurred. The existing 24-trial proposal results remain unchanged; the Proposal task was notified.

Review-only candidate: same 80/20 workload, hot-count maps 8/4/0 and 4/4/4, three consumers, keep versus redistribution policy, two paired repetitions per condition, eight trials. All consumers initially have 20 partitions. Candidate 900 messages/s total, 600 seconds production including 60 seconds warm-up, 120 seconds drain, decision 120 seconds after evaluation begins. The 400 messages/s capacities in fixtures are illustrative only. Final settings require separate calibration and user confirmation.

Large raw experiment storage is unchanged. No new raw experiment evidence exists for this preparation.
