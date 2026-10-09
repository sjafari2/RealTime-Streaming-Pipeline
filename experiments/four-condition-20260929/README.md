# Twelve-hot-partition comparison

This protocol compares four scheduled responses at **700 aggregate messages/s**. The workload sends 80% of input to twelve of sixty partitions: an 80/20 distribution. All twelve hot partitions initially belong to Consumer 2. Processing is 2,000 SHA-256 iterations per record, with no added sleep.

| Response | Consumers | Resulting assignment rule |
|---|---:|---|
| Keep three | 3 | Retain the starting assignment |
| Redistribute within three | 3 | Four hot and sixteen cold partitions per consumer |
| Targeted scaling | 3 to 6 | Two hot and eight cold partitions per consumer |
| Native Kafka scaling | 3 to 6 | Cooperative-sticky group assignment without a targeted map |

Each condition has two trials with one-minute warm-up, ten-minute evaluation and two-minute drain. The action is scheduled one minute into evaluation. Condition order is reversed in Run 2; seeds 81 and 82 identify the two workload schedules.

Preparation captures a native empty-topic map, selects the twelve lowest-numbered partitions owned by Consumer 2 and saves that hot set. Original pod identities, nodes, resources and ownership are checked before traffic in every condition. Native scaling and explicit targeted handover use different coordination mechanisms; the comparison includes those differences.

## Preview and execution

```bash
python3 experiments/four-condition-20260929/run.py
```

This previews the original protocol, including its 99 ms runtime deadline. The published one-second and half-second outcomes were reconstructed retrospectively from the same event evidence. A new protocol with the current one-second target is previewed with `--slo-ms 1000`; it does not change the historical trials.

Execution adds `--execute` and a passed [monitoring check](../monitoring-gap-20260929/README.md):

```bash
python3 experiments/four-condition-20260929/run.py --slo-ms 1000 --execute \
  --monitoring-gate results/TECHNICAL_RUN/monitoring-gap-verification.json
```

The technical-run path is a placeholder. Empty-topic preparation and a low-load native scaling check precede performance trials. Failed preparations are retained separately; valid unfavorable performance outcomes are retained. The runner restores the saved shared configuration and returns to three stopped consumer applications. `--resume results/CAMPAIGN_DIRECTORY` accepts only a validated, restored partial campaign with an unchanged protocol.

## Analysis

```bash
python3 experiments/four-condition-20260929/summarize.py \
  results/CAMPAIGN_DIRECTORY results/comparison-analysis --growth-window-samples 5
```

Five two-second intervals give a ten-second plotted growth window. Missing observations, ownership changes and offset resets interrupt it; the fifteen-observation skew window and separate thirty-second recovery hold retain their own definitions. The [protocol](protocol.json) preserves the original design. [Published results](../../experiment-records/four-condition-20260929/README.md) include measurement limitations and links to the numerical evidence.
