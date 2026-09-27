# Balanced 900 messages/s: repeated stability trial

Both trials used three producers (300 messages/s each), three consumers, 60 partitions, 2,000 SHA-256 iterations per message, no added sleep, balanced input and no scaling. Each used 60 seconds of warm-up, 1,200 seconds of evaluation production and 120 seconds of drain.

| Measurement | Run 1 | Run 2 |
|---|---:|---:|
| Evaluation messages | 1,080,001 | 1,080,000 |
| Completed by drain | 1,080,001 | 1,080,000 |
| Unfinished by drain | 0 | 0 |
| Completion p99 (s) | 47.400 | 51.411 |
| Time-weighted mean lag (offsets) | 7,319.5 | 7,705.2 |
| Peak sampled lag (offsets) | 14,166 | 15,315 |
| Final 10-minute backlog growth (offsets/s) | 11.482 | 12.691 |
| Valid evaluation lag coverage | 99.83% | 99.83% |

Both runs showed sustained backlog growth during the 20-minute production interval, despite completing all evaluation messages during drain. Run 2 therefore reproduces the qualitative result of Run 1 under the tested settings.

Run 2 was recovered from the original retained Prometheus history and original producer/consumer evidence after the initial collection timeout. No replacement traffic was generated during recovery. Outcome and resource-measurement audits passed. Process CPU and RSS are retained in the measurement audit; these are not container or node measurements. The pre-campaign configuration was restored and the stopped state verified. The initial campaign failure remains preserved separately.

Completion p99 includes only admitted evaluation messages completed before the drain cutoff; unfinished counts are reported alongside it. Lag is high offset minus returned-record position. Growth uses processing backlog over covered same-owner segments. Missing or stale observations remain gaps. Finishing during drain does not by itself demonstrate stability during continuous production; these finite observations on shared machines do not establish a universal capacity threshold.

[Run 2 lag PNG](rate900-run2-lag.png) · [PDF](rate900-run2-lag.pdf) · [Comparison, audit and evidence hashes](rate900-run2.json)

Raw evidence is retained separately; versioned summaries and hashes are not a raw-data backup.
