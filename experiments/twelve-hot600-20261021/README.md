# Twelve high-input partitions at 600 messages/s

Eight trials compare Keep 3, Redistribute 3, Targeted Scale 6 and Kafka Scale 6, each twice. Three producers target 200 messages/s each. Eighty percent of input goes evenly to twelve of sixty partitions initially owned by Consumer 2; the remaining partitions share twenty percent.

The rate, client resource declarations, application work and measurement boundaries match the October 21 four-partition 600 messages/s block. Pods are recreated for this campaign, so placement is recorded again and is held consistent within the new comparison; a cross-campaign difference is not an isolated effect of hot-partition count.

Each trial uses 60 seconds of warm-up, 600 seconds of evaluation and 120 seconds of drain. Actions are scheduled at evaluation second 60. Run 1 uses seed 81; Run 2 uses seed 82 and reverses condition order. Targeted redistribution assigns four hot partitions to each of three consumers, or two to each of six. Native Kafka scaling has no targeted assignment map.

Before execution, verify client resource declarations, source hashes, monitoring and the saved idle run control. Empty-topic ownership capture and restart checks precede a low-load native scaling check. Failed preparations are retained separately. All valid performance trials, including unfavorable outcomes, remain in the result set.

## Run and analyze

```bash
python3 experiments/four-condition-20260929/run.py --aggregate-rate 600 --hot-partitions 12 --slo-ms 1000
python3 experiments/four-condition-20260929/run.py --aggregate-rate 600 --hot-partitions 12 --slo-ms 1000 --execute --monitoring-gate /path/to/passed-monitoring-gap-verification.json
```

The second command requires the verified monitoring record and three ready producers and consumers. It records the exact code revision and saves run evidence under `results/`. The coordinator restores the original shared configuration after execution. Scale both client StatefulSets to zero when collection is complete to avoid reserving idle resources.

Report whole-run cohort completion latency, unfinished work and one-second SLO violations alongside throughput, backlog, skew, growth, recovery, intervention disruption and observed/requested CPU and memory. Preserve before-action (seconds 30–60), activation-to-end and final-window (570–600) statistics. Warm-up work remains queued; it is excluded from evaluation-message latency but included in completion throughput when processed during a measurement window. Historical 700 results remain a separate deployment block.

The [protocol](protocol.json) specifies the design before any new performance results. Use the existing four-condition summary and completion-window analyzers after the campaign, then add the verified results and plots to the twelve-partition proposal tables with the rate printed on every row.
