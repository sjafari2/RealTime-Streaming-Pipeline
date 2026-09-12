# run-20260912-063614

No-workload readiness diagnostic under protocol revision 3. Kafka positions on the fresh empty topic were unresolved, so the first pre-production check blocked release. No producer workload or scheduled scaling occurred. All 18 evidence files match their Nautilus originals. Excluded from performance comparisons.

Managed status: **interrupted_or_failed**. Full data: `results/run-20260912-063614/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 21, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 600 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

This attempt is excluded from performance comparisons. Retained failures are:
- The command did not complete its scheduled run.
