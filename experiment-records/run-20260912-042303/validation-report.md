# run-20260912-042303

Excluded replica-control diagnostic: three consumers were requested but the existing HPA restored six. Production was interrupted; a second guard signal complicated cleanup. The retained event evidence has now been verified against all 27 original PVC files. This is not a valid three-consumer or full-duration performance trial.

Managed status: **interrupted_or_failed**. Full data: `results/run-20260912-042303/` in the active code folder; original event evidence remains on the Nautilus PVCs.

The frozen configuration uses 3 producers, 3 initial consumers, 60 partitions, balanced input, seed 1, and 0 SHA-256 iterations per message. The target is 12,000 messages/s total. Production lasts 180 seconds, with 60 seconds of warm-up and a 120-second bounded drain.

This attempt is excluded from performance comparisons. Retained failures are:
- Requested three consumers, but the existing HPA restored six. Operator ended production. A monitoring guard issued a second interrupt during cleanup; this recovery retained the same run evidence. Exclude from performance comparisons.
