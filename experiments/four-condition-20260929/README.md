# Four responses to concentrated hot-partition ownership

This block compares four scheduled responses at an aggregate target of 700 messages/s. Each trial has 60 seconds warm-up, 600 seconds evaluation, and 120 seconds drain (13 minutes total). There are two trials per condition; the second run reverses condition order. Seeds 81 and 82 identify the two workload schedules.

| Condition | Consumers | Scheduled response |
|---|---:|---|
| Keep three | 3 | Keep the starting partition ownership |
| Redistribute within three | 3 | Four hot and sixteen cold partitions per consumer |
| Scale and redistribute | 3 → 6 | Two hot and eight cold partitions per consumer |
| Kafka scaling | 3 → 6 | Kafka's classic cooperative-sticky rebalance; no targeted assignment |

The action is scheduled 60 seconds after evaluation begins. The workload sends 80% of traffic to twelve of sixty partitions. All twelve initially belong to Consumer 2. The application performs 2,000 SHA-256 iterations per record without an added sleep.

## Comparable starting conditions

The preparation captures Kafka's normal three-consumer assignment on an empty topic. The twelve lowest-numbered partitions initially owned by Consumer 2 become the frozen hot set. All conditions use that hot set and starting map. Original pod identities, hosting nodes, resource declarations, and partition ownership are checked before workload release. This avoids imposing a manually chosen starting map that Kafka's built-in assignor cannot reproduce. Matching does not eliminate variability on shared machines.

Kafka scaling uses a normal consumer group with a unique static identity per pod. The other three conditions use the existing explicit exclusive-ownership adapter. Their coordination mechanisms differ, so this is a comparison of the implemented responses, not an isolated test of replica count alone. Normal Kafka scaling can also move hot partitions; it is not scaling with ownership held fixed.

The procedure admits no performance trial until the monitoring correction passes its live validation. A matching empty-topic restart and a low-load native scaling technical trial precede the performance block. Technical validations and rejected preparations do not count as performance trials. Only an ownership mismatch before traffic may be retried, at most three times; runtime and evidence failures stop the block. Valid unfavorable outcomes remain in the results.

## Evidence and interpretation

`protocol.json` fixes timing, workload, measurement definitions, intervention-cost boundaries, recovery thresholds, and failure rules before performance trials. The runner saves configurations, the starting reference, action journals, run identifiers, source revision, validation outcomes, and independent resource observations. The underlying managed runner retains producer acknowledgments, completion and assignment events, clock checks, and Prometheus exports.

Completion p99 uses distinct acknowledged evaluation messages completed by the drain cutoff. Warm-up records are excluded from that cohort but remain part of system backlog. Unfinished work is reported alongside completion latency. Growth, skew, CPU, memory, throughput, requested resources, processing interruption, transition backlog accumulation, and recovery are retained. Genuine monitoring gaps remain unavailable.

For native Kafka rebalancing, some consumers can continue working while others transfer partitions. Per-partition handover delays are therefore reported without assuming a pipeline-wide processing pause. Completion during drain and recovery while input continues are separate outcomes.

## Reproducing the figures

The current growth figure uses a 10-second window for every condition and run.
It is recomputed from the retained lag snapshots using five 2-second intervals.
Missing data and ownership/offset changes still interrupt the curve; after a
break, growth requires ten seconds of fresh contiguous observations. The
15-snapshot skew mean and the separate 30-second recovery criterion are unchanged.
This is an analysis update after the trials; the original observations and
run summaries remain unchanged.

```bash
python3 experiments/four-condition-20260929/summarize.py \
  results/four-condition-20260929-015500 \
  experiment-records/four-condition-20260929 \
  --growth-window-samples 5
```

For a sensitivity comparison, 10 intervals give 20 seconds and 15 give the
original 30 seconds. Use a separate output directory to preserve the main figures.

## Execution

Dry-run review:

```bash
python3 experiments/four-condition-20260929/run.py
```

After inspecting a passed monitoring-gate record:

```bash
python3 experiments/four-condition-20260929/run.py --execute \
  --monitoring-gate experiment-records/monitoring-gap-20260929/live-verification.json
```

The runner restores the original shared configuration and returns to three stopped consumer applications. Local source must be committed and deployed before execution. Large raw evidence stays under the ignored `results/` directory and on the shared evidence volumes; small reviewed results are versioned separately.

## Resuming a restored campaign

A collection failure can be repaired from existing evidence without rerunning traffic. Resume accepts only an unchanged protocol, passed monitoring/native-scaling checks, a verified configuration restoration, and an unrepeated prefix of validated trials. It skips those trials and preserves the frozen starting reference. Resource observers use separate files for resumed segments; missing intervals are never interpolated. The temporary `caffeinate -is` assertion also prevents system sleep while on AC power and ends with the runner.

```bash
python3 experiments/four-condition-20260929/run.py --execute \
  --monitoring-gate experiment-records/monitoring-gap-20260929/live-verification.json \
  --resume results/<campaign-directory>
```
