# Interpretation of the completed comparison

All three scheduled responses relieved the concentrated workload in both runs. Keeping the initial assignment left 34.64% and 34.83% of evaluation messages unfinished after the fixed drain. Every intervention completed the full evaluation cohort and met the predefined low-backlog recovery condition while input was still arriving. This extends the evidence beyond completion during drain alone.

Redistributing the twelve hot partitions among the three existing consumers reduced completion p99 from 366.64 to 80.10 seconds in Run 1 and from 362.79 to 84.16 seconds in Run 2. It used 72 requested CPU core-minutes and 36 requested GiB-minutes over evaluation plus drain. The two six-consumer approaches used approximately 137 core-minutes and 68.4–68.6 GiB-minutes. Redistribution therefore achieved complete outcomes with less requested capacity in this block; requested resources are not measured CPU consumption or monetary cost. The first baseline's partial request integral must not be used as a full-run resource cost.

Scaling with Kafka's normal cooperative rebalance had the lowest p99 in both runs: 71.41 and 72.46 seconds. Scaling with targeted redistribution produced p99 values of 100.81 and 107.24 seconds. The targeted combined response reached the recovery condition sooner than redistribution within three consumers in both runs, but its higher p99 shows that faster recovery does not necessarily imply lower whole-cohort tail latency. Recovery after native scaling varied between runs. These observations do not establish a universal best response.

The treatments differ in coordination as well as ownership. Explicit redistribution uses a global release/acquire/resume barrier and currently releases every explicit owner, including owners of unchanged partitions. Its target map balances hot and cold partition counts separately; it is not a minimum-movement or capacity-weighted optimizer. Native cooperative rebalance can keep other partitions processing during handover. Its near-zero global no-processing interval does not mean each partition was continuously served. Preparation and coordination timing contribute to the observed outcomes, so these differences cannot be attributed solely to replica count or the final partition map.

Equal partition counts also did not yield equal hot-partition counts. Native scaling ended with ten partitions on each consumer, but the hot counts for Consumers 0–5 were 0, 0, 2, 4, 2, 4 in Run 1 and 0, 0, 2, 4, 3, 3 in Run 2. Targeted redistribution assigned four hot partitions per consumer with three consumers, or two per consumer with six. Actual process CPU and memory plots provide additional context; requested resources do not establish equal processing capacity.

The skew ratio must be read with the absolute lag. After most backlog is removed, a small number of queued records can produce a high maximum-to-mean ratio. A higher ratio at low total lag is not by itself evidence that the intervention failed. Negative backlog growth means observed queued work is decreasing; throughput can temporarily exceed the input rate while accumulated work is processed.

Recovery is the first confirmation of at most 100 backlog offsets for thirty consecutive valid seconds during continuing production, with unchanged ownership and nondecreasing offsets. It is not proof of permanent stability. The 50- and 200-offset sensitivity results remain in the machine-readable record. Completion p99 describes distinct acknowledged evaluation messages completed by the observation cutoff, measured through application completion before commit acknowledgment. Each plotted condition is a separate trial, not a before/after p99 within one trial.

The common starting map and original consumer pods/nodes were verified before traffic, and runtime source/dependency signatures matched across all eight trials. The second run reversed treatment order. Two runs, shared-machine variability, and unfrozen placement of newly added replicas still limit causal attribution and generalization. Comparison with the earlier eight-minute blocks is not an isolated test of duration: dates, hot-partition identifiers and monitoring implementation also changed. All responses remain scheduled; adaptive action choice and hot-key splitting are future work.

The next research question is whether a controller can select a response using measured capacity, backlog and intervention cost, and whether more selective handover or capacity-aware assignment adds value over the successful native-scaling and three-consumer redistribution baselines. No additional trials are included in this completed block.

The [results and metric definitions](README.md), [validation record](validation.json), and [raw-file recovery record](local-evidence-recovery.json) document the evidence and limitations. Large raw evidence remains outside Git; public summaries and hashes are not a separate backup of that evidence.

## Why retain the ten-minute comparison as the primary result

The longer observation captures recovery while input continues. In the two
redistribute-within-three trials, the predefined low-backlog criterion was
confirmed at 372 and 396 seconds after evaluation began (6.2 and 6.6 minutes),
after a five-minute evaluation would have ended. The ten-minute block also
compares all four responses, including native Kafka scaling. A finite observation
does not prove indefinite stability.

Monitoring completeness is a separate issue. The earlier five-minute
redistribution trials first regained valid total lag 79.74 and 72.22 seconds
after active ownership verification. The corresponding later values were 1.45
and 0.88 seconds after the monitoring correction. Longer duration alone did not
fix missing measurements. Both blocks now display ten-second growth; this
reduces the extra window wait by twenty seconds without reconstructing missing
observations. Execution date, hot-partition identities and monitoring code also
differ, so the blocks are not an isolated comparison of run duration.

The proposal uses this ten-minute block as its main intervention comparison,
with the earlier five-minute evidence retained in the appendix. Both durations
have the same available diagnostic types: lag, growth, skew, process CPU/RSS,
throughput, p99/unfinished outcomes, final ownership and event-based outstanding
work, alongside requested-resource and intervention-cost tables. There is no
separate five-minute native-Kafka-scaling trial.
