# Four-hot-partition comparison

This block keeps the aggregate rate at 700 messages/s and directs 80% of input to **four of sixty partitions**, initially owned by Consumer 2. This is a stronger concentration than the twelve-partition 80/20 workload.

Two runs compare redistribution within three consumers with targeted scaling to six. Both use explicit ownership handover. Hot partition counts after the action are 2/1/1 with three consumers and 1/1/1/1/0/0 with six. The rule balances hot and cold partition counts separately; it is not an optimal assignment based on measured capacity.

Each trial lasts thirteen minutes: one warm-up, ten evaluation and two drain. The action is scheduled one minute into evaluation. Seeds 81 and 82, reversed condition order, and verified common starting conditions follow the twelve-partition comparison. There are no no-action or native-scaling performance arms in this block.

The one-second completion deadline and half-second sensitivity threshold were specified before execution. Neither is a validated application SLA. The late production cohort, evaluation seconds 480–600 followed to the original cutoff, was also specified before these trials.

```bash
python3 experiments/four-condition-20260929/run.py \
  --aggregate-rate 700 --hot-partitions 4 --targeted-only --slo-ms 1000
```

This prints the design. Execution adds `--execute --monitoring-gate results/TECHNICAL_RUN/monitoring-gap-verification.json`, using a passed [technical check](../monitoring-gap-20260929/README.md). The runner stops on invalid evidence and restores the prior shared configuration.

The [protocol](protocol.json) retains the prospective design and [metric coverage](METRIC_COVERAGE.md) describes the diagnostics. [Published results](../../experiment-records/four-hot-partitions-20260930/README.md) retain both runs, including the late-period backlog spike in the first targeted-scaling trial.
