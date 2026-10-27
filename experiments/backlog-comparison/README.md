# Starting backlog and mitigation

This experiment compares four scheduled responses at the same continuing input rate: keep three consumers, redistribute within three, scale to six with Kafka rebalancing, and scale to six with targeted redistribution. It does not split keys.

Twelve of sixty partitions receive 80% of input and initially belong to one consumer. Two warm-up input rates create different amounts of accumulated work. Both conditions return to 600 messages/s total at evaluation start. Each run has one minute of warm-up, ten minutes of evaluation and two minutes of drain. The response is scheduled at evaluation second 60. Thus the current input rate is the same throughout the before-action measurement window and after the action.

The specification records the selected rates, initial hot owner, balanced calibration and pilot evidence. Pilots must demonstrate different backlogs at the decision point and verify the rate changes before the sixteen performance trials begin. Different preparation histories also change the number and ages of queued messages; those differences are retained in the analysis. The labels “smaller” and “larger” describe measured conditions, not exact prescribed backlog sizes.

Each response is repeated twice per condition, using the same seed across responses in a repetition. The second repetition reverses both response and backlog-condition order. Initial partition ownership and the original consumers' identities, locations and resources are checked before traffic. Added consumer locations are recorded. The target maps balance hot and cold partition counts separately; they are not claimed to be capacity-optimal.

Report the actual pre-action backlog, consumer arrival rates and eligible capacity measurements alongside completion latency, unfinished work, deadline outcomes, throughput, recovery, resource use and interruption. Follow messages pending at the decision point separately from messages produced afterward. Completion is measured before offset-commit acknowledgment. Recovery requires total processing backlog at or below 100 offsets for 30 seconds of valid, unchanged-ownership observations during continuing input; completing during drain is a separate outcome.

Run the validated specification from the repository root:

```bash
python3 experiments/backlog-comparison/run.py \
  --specification path/to/validated-specification.json \
  --monitoring-gate path/to/monitoring-gate.json --execute
```

The existing campaign coordinator performs empty-topic ownership checks and a low-load scaling check before admitting performance trials. It retains rejected preparations and unfavorable valid outcomes, checks each trial, and restores the shared configuration. Local message logs are compressed only after byte verification; the original cluster evidence is retained. Use the recorded campaign directory with `--resume` only after inspecting and resolving a technical failure.
