# Aborted empty preparation — retained, no performance data

The first empty capture passed (`run-20260913-022024`). The second stage stopped when a Kubernetes pod-list command returned exit 1 before a new managed run was created. The original exception did not retain its stderr in the block log, so the precise API failure cause is unknown. A later read-only API check succeeded.

No performance trials ran. Original configuration, HPA settings and replica counts were restored and verified. All nine application processes were independently confirmed stopped. All 18 collected evidence files match their PVC hashes, and the empty preparation passed its zero-traffic check. A lossless local archive was verified.

One fresh block is being attempted under the existing authorization to repeat the pair. This is an explicitly recorded operational restart after a pre-production API failure, not an ownership retry or outcome-based selection. The aborted block remains failed. The fresh block retains four empty preparations, no within-block retries, the 1,200-second preparation bound and the same workload. Stop if the fresh block fails again.
