# Redistribution-only comparison

Four performance trials are planned at 700 aggregate messages/s: two keep-three trials and two redistributions among the same three consumers. Every trial starts with twelve hot partitions on Consumer 2. The treatment assigns four hot and sixteen cold partitions per consumer, without adding replicas.

The nonempty handoff validation passed: all 167,986 acknowledged messages completed, with no duplicate completions, unmatched identities or gaps in completed offset prefixes. This technical check is separate from the performance count. Interruption, outstanding-work accumulation and recovery criteria were specified before this block.

[Protocol](../../experiments/c2-redistribution-20260928/README.md) · [Cost definitions and capture requirements](../../experiments/c2-redistribution-20260928/cost-protocol.json)
