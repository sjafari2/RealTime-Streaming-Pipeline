# Starting backlog and mitigation

This study compares four scheduled responses under smaller and larger accumulated backlogs. It asks whether a response that handles the continuing input also clears the existing work promptly, and what resources and disruption that recovery requires.

The evaluation rate is 600 messages/s across three producers. Twelve of sixty partitions receive 80% of input and initially belong to the original consumer with the lowest eligible measured capacity. The hot set and initial assignment remain fixed within the comparison. Consumer capacity is estimated separately from two 60-second intervals with sustained processing backlog and stable completion rates. These measurements describe the deployed consumers, not a universal capacity limit.

| Setting | Specification |
| --- | --- |
| Warm-up | 60 seconds; 300 or 900 total messages/s |
| Evaluation | 600 seconds; 600 total messages/s in both conditions |
| Scheduled decision | 60 seconds after evaluation starts |
| Drain | 120 seconds after production stops |
| Responses | Keep 3, redistribute within 3, scale to 6 with targeted redistribution, scale to 6 with Kafka cooperative rebalancing |
| Repetitions | Two per response and backlog condition; 16 performance trials |
| Workload seeds | 91 and 92; common seed within each repetition |
| Processing | Sequential stateless task, 2,000 SHA-256 iterations per message, no additional sleep |

The second repetition reverses the order of both backlog conditions and responses. Evaluation partition selection uses a separate deterministic sequence, independent of warm-up message count. Preparation rates are fixed before performance trials; actual backlog and message age at the decision are reported. The two histories change both backlog amount and age, so they are not interpreted as an isolated manipulation of queue size.

The smaller preparation sends 100 messages/s from each of three producers during warm-up; the larger preparation sends 300 messages/s per producer. Consumers process throughout warm-up, and queued records remain when evaluation begins. At that boundary every producer switches to 200 messages/s. The original assignment stays in place for another 60 seconds before the scheduled response, allowing the same continuing input to act on different retained backlogs.

`prepare.py` verifies readiness, the initial assignment, busy-capacity evidence and the two preparation histories. A successful specification records the measured backlogs and the matching evaluation traces. `run.py` then reuses the managed runner, verifies the original pod identities, locations and resources, and checks native scale-up before admitting the sixteen trials. Every trial uses a fresh topic and a saved configuration. Preparation failures remain separate from performance trials; valid unfavorable outcomes remain in the results.

```bash
python3 experiments/backlog-comparison/prepare.py \
  --calibration-reference RUN_ID --execute

python3 experiments/backlog-comparison/run.py \
  --specification results/PREPARATION/validated-specification.json \
  --monitoring-gate PATH_TO_PASSED_MONITORING_GATE \
  --backup-root /path/to/raw-evidence-backup --execute

python3 experiments/backlog-comparison/summarize.py \
  results/CAMPAIGN results/BACKLOG_ANALYSIS
```

The campaign generates its report after all planned trials and configuration restoration pass. The backup option preserves and verifies raw records between runs, then includes the final validation records and campaign metadata at completion. `--reuse-capacity` on the preparation command permits repeating the two pilot preparations after a producer-only correction, provided the previously validated original pods, placement, resources and processing settings are unchanged. Changes to consumer processing require new capacity measurements. `--partial` on the summary command produces an explicitly provisional report from the completed prefix of a running campaign.

Whole-run completion latency covers evaluation messages completed by the drain cutoff. Unfinished and deadline outcomes retain the full admitted evaluation cohort. Completion-window statistics describe messages completed within the before, activation-to-end and final-evaluation intervals. They do not replace whole-run outcomes. Signal means give valid samples equal weight; missing observations remain missing. Requested resources are integrated over time and remain separate from measured process CPU and RSS.

The reporting intervals follow the proposal: **Before** is evaluation seconds 30–60, **Active–end** is confirmed assignment activation through second 600, and **Eval. end** is seconds 570–600. Keep 3 uses 60–600 for Active–end. `REPORTING_INTERVALS.md` and `reporting-intervals.json` retain throughput, p99, mean backlog, mean skew, endpoint pending counts and signal coverage for each interval and run. Separate interval plots use these same labels. The p99 time series uses consecutive 30-second completion intervals; only its final point represents Eval. end, and the series is not a mean of percentiles.

Full-scale plots retain all observed values. Backlog ticks are spaced every 2,500 offsets, with labels every 25,000 offsets to keep the full-range overview readable. Throughput ticks and labels are spaced every 500 messages/s. These settings change tick spacing, not axis limits.

`analyze_cohorts.py` also follows records already acknowledged but unfinished at the common decision time, including warm-up records. Its clearance curve retains the entire pending cohort in the denominator. Records produced afterward form a separate cohort; records produced earlier but acknowledged later are retained separately. Recovery requires processing backlog at or below 100 offsets for 30 continuous valid seconds while input continues. Finishing during drain alone does not establish that recovery criterion.

Raw evidence is compressed only after byte verification. Small reviewed summaries and evidence hashes belong in `experiment-records`; raw records require a separate verified backup. Added consumers can occupy different nodes across runs and do not inherit the original consumers' capacity estimates. The reported comparison concerns these scheduled implementations and two repetitions, not a validated adaptive controller.
