# Balanced reference at 600 messages/s

Run `run-20261026-224028` used three consumers, sixty partitions, 2,000 SHA-256 iterations per message, and no mitigation. The schedule was one minute of warm-up, twenty minutes of evaluation and two minutes of drain.

All 720,000 acknowledged evaluation messages completed. The admitted rate was 600 messages/s; completion throughput during evaluation was 600.009 messages/s, including warm-up messages completed during evaluation. There were no failed sends, duplicate completion attempts or invalid completion-clock observations. Valid lag observations covered 99.67% of evaluation.

Processing backlog reached 2,137 offsets in an early transient, then recovered. The arithmetic means of valid processing-backlog samples in minutes 5–10, 10–15 and 15–20 were 47.61, 48.95 and 49.91 offsets. The final ten-minute covered trend was 0.013 offsets/s, corresponding to a change from 40 to 48 offsets. The saved trace supports using 600 messages/s as a reference for this deployment and observation period.

Whole-cohort completion latency had a mean of 0.174 seconds, p99 of 2.126 seconds and maximum of 19.321 seconds. The one-second deadline-miss fraction was 1.493%, with no unfinished messages at the cutoff. Bounded backlog does not imply that every message meets its deadline.

The graph distinguishes processing backlog from consumer lag and includes a clearly labelled detail panel. Five-minute arithmetic sample means are separate from the time-weighted diagnostic means retained in `stability-summary.json`. Original evidence remains on the cluster and in the separately retained local run directory; hashes identify the source measurements. This calibration is separate from the planned sixteen performance comparisons.
