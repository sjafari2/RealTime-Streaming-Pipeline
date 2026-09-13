# 80/20 consumer-scaling comparison

Completed on 13 September 2026: four empty rehearsals and both trials passed; original settings were restored. [Results and plots](../../experiment-records/8020-comparison-20260913/README.md). Authorized by the user on 13 September 2026. This is a new workload comparison, not a relabeling of the earlier single-partition results.

Three producers target 1,500 messages/s total. With common workload seed 71, 80% of messages are aimed uniformly at 12 of 60 partitions, and the other 20% uniformly at the remaining 48. The selected partition IDs are 5, 10, 25, 33, 35, 39, 43, 45, 51, 55, 58, 59. Expected rates are 100/s per selected partition and 6.25/s per other partition; observed proportions fluctuate. The producer routes the remainder only to the cold subset.

Use the already validated static-startup procedure: four empty rehearsals, then scale from three to six followed by keep-three, with the same frozen original pod identities and complete starting ownership. No new node-placement constraints. Stop on first failure, no retries, 1,200 seconds cumulative preparation, at most six starts.

Both trials use 300 seconds production including 60 seconds warm-up, then 120 seconds drain; the scheduled decision is production +120 seconds. Preserve 100-byte payload setting, 2,000 SHA-256 iterations and no added sleep. This isolates the workload-routing change from the preceding single-partition block. It does not assume overload: measure actual lag and report whether scaling was warranted at this rate. Do not increase the rate or tune against outcomes during the pair.

Run from the project workspace using the final script:

```bash
/tmp/pipeline-review-venv/bin/python /Users/soheila/Desktop/Thesis-26-27/code/experiments/controlled-followup-20260912/execute_block.py --static-startup --include-comparison --workload 80-20 --audit-dir /ABSOLUTE/NEW/AUDIT/DIRECTORY
```

The audit directory must be new. Save per-run evidence, plots, exact cohort outcomes, unfinished/deadline outcomes, lag and resource coverage, and restoration records. Completion remains before commit acknowledgment. One pair is preliminary evidence, not independent replication or a general causal conclusion.
