# run-20260912-062803

Interrupted pre-action query diagnostic under protocol revision 2. The combined PromQL omitted required sample timestamps; the guard stopped production before scheduled scaling. The application did not fail. All 18 original files match the Nautilus copies. Excluded from planned full-duration and scaling comparisons.

Managed status: **interrupted_or_failed**. Full data: `results/run-20260912-062803/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 21, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 600 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

This attempt is excluded from performance comparisons. Retained failures are:
- The command did not complete its scheduled run.
