# Redistribution-only comparison

Four performance trials are planned at 700 aggregate messages/s: two keep-three trials and two redistributions among the same three consumers. Every trial starts with twelve hot partitions on Consumer 2. The treatment assigns four hot and sixteen cold partitions per consumer, without adding replicas.

The nonempty handoff validation passed: all 167,986 acknowledged messages completed, with no duplicate completions, unmatched identities or gaps in completed offset prefixes. This technical check is separate from the performance count. Interruption, outstanding-work accumulation and recovery criteria were specified before this block.

[Protocol](../../experiments/c2-redistribution-20260928/README.md) · [Cost definitions and capture requirements](../../experiments/c2-redistribution-20260928/cost-protocol.json)

## Completed performance trials

2 of four performance trials are complete. In-progress trials are not included below.

| Run | Condition | Completion p99 (s) | Unfinished (%) |
|---|---|---:|---:|
| 1 | Keep 3 | 226.34 | 30.18 |
| 1 | Redistribute within 3 | 103.76 | 0.00 |
