# run-20260912-052133

Failed readiness attempt, preserved for audit. Two consumers stopped after generation-related commit errors before the common production barrier was released. The consumer correction is documented in docs/commit-handling.md; this attempt supplies no performance result.

Managed status: **failed_before_workload**. Full data: `results/run-20260912-052133/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 11, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

This attempt is excluded from performance comparisons. Retained failures are:
- Readiness failed after asynchronous ILLEGAL_GENERATION commit errors. No shared production start was released.
