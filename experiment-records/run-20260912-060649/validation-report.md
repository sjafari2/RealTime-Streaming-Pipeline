# run-20260912-060649

Interrupted technical diagnostic under protocol 1. Scheduled scaling reached six consumers, but the legacy wall-clock freshness guard stopped production early. Excluded from planned full-duration comparisons. All 27 original files match their Nautilus copies; no previous gaps or outcomes have been repaired.

Managed status: **interrupted_or_failed**. Full data: `results/run-20260912-060649/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 21, and 2000 SHA-256 iterations per message. The target is 1,500 messages/s total. Production lasts 600 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

This attempt is excluded from performance comparisons. Retained failures are:
- The command did not complete its scheduled run.
