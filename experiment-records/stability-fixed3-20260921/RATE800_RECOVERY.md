# Recovered 800 messages/s stability trial

The original run `run-20260923-023907` was recovered from retained Prometheus history after restoring Kubernetes authentication. No replacement performance trial was run. The initial export failure and campaign failure records remain preserved; offline validation now passes with no outcome or measurement-audit issues.

Three consumers processed balanced input across 60 partitions without scaling. Three producers each targeted 800/3 messages/s (approximately 266.667), totaling 800 messages/s. The trial used one minute of warm-up, 20 minutes of evaluation production and two minutes of drain, with 2,000 SHA-256 iterations per message and no added processing sleep.

| Measurement | Result |
|---|---:|
| Evaluation messages admitted and completed by drain | 960,000 / 960,000 |
| Unfinished evaluation messages | 0 (0%) |
| Completion p99 | 0.585 seconds |
| Mean completion latency | 0.099 seconds |
| Time-weighted mean lag | 68.5 offsets |
| Peak sampled lag | 449 offsets |
| Final ten-minute processing-backlog growth | +0.0084 offsets/s |
| Valid evaluation lag coverage | 1,192 / 1,200 seconds (99.33%) |
| Evaluation process CPU/RSS coverage, each process and metric | 99.83% |

The plotted lag fluctuated around a low level with short spikes that subsided, rather than sustained accumulation. This resembles the 600 and 700 messages/s observations; the tested 900, 1,200 and 1,500 messages/s runs showed sustained growth. These observations do not establish a universal capacity threshold or infinite-horizon stability on shared infrastructure. One recovered 800 trial is sufficient to retain this observation; further repetitions would strengthen reproducibility, not repair this export failure.

Lag is high offset minus returned-record position; the growth calculation uses processing backlog. Completion is measured before offset-commit acknowledgment. Gaps remain gaps, and the exact evaluation-boundary lag is unavailable. Process CPU and RSS measurements are available; these are not broker, node or container resource measurements.

[Evaluation lag PNG](rate800-run1-lag.png) · [PDF](rate800-run1-lag.pdf) · [Summary and evidence hashes](rate800-run1.json)

Large raw evidence remains in separate results storage. Versioned hashes and summaries do not replace a raw-data backup.

After recovery, the expired run was stopped and the pre-campaign configuration was restored and verified. The original campaign failure remains unchanged; a separate restoration-recovery record documents the successful cleanup.
