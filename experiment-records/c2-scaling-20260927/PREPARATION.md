# Consumer 2 scale-up preparation

Two technical handoff checks passed on Nautilus before the controlled comparison. They completed all 167,968 and 167,986 acknowledged messages, respectively, without duplicate completions, unmatched identities or gaps in the completed offset prefixes. The first check had approximately 29,000 offsets of queued work before transfer. New replicas waited without ownership until the original owners had released their verified completed offsets.

Between these checks, CPU/RSS sampling was moved ahead of the paused-processing branch so it continued during handoff. An independent observer recorded consumer resource requests every two seconds; the second check covered its complete evaluation-and-drain interval. Lag observations were temporarily unavailable during ownership transfer and while new fetch positions became valid. Those gaps remain visible and are not treated as zero backlog.

One intervening preparation stopped on fresh-topic leader metadata errors before releasing any production. A bounded metadata refresh was added for the specific transient leader errors; authorization failures and deadlines still stop preparation. All 258 local tests passed after the changes. The first campaign was deliberately paused after its successful technical validation; neither that pause nor the empty failed preparation is counted as a performance trial.

The performance comparison starts every trial with all twelve hot partitions on Consumer 2, at 700 aggregate messages/s. It compares keeping three consumers with scheduled scaling to six and an explicit redistribution to two hot and eight cold partitions each. The four performance trials are now complete, two per condition; see [results and plots](README.md). This combines capacity and assignment changes; it is not an isolated test of added capacity or Kafka's automatic assignor.

See [reviewed protocol](../../experiments/c2-scaling-20260927/README.md) and [validation records](preparation-checks.json). Raw evidence stays in separate results storage. These checks support this synthetic pilot; they do not establish exactly-once external effects or production fault tolerance.
