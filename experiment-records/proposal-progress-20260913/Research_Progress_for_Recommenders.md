# Kafka stream processing research progress

Soheila Jafari Khouzani | University of New Mexico | 13 September 2026

I have implemented and exercised a Kafka experiment pipeline on the Nautilus Research Cloud and completed 24 performance trials. The results show that the benefit of adding consumers depends on how work is distributed and on the processing capacity available after scaling. This provides a measured foundation for my proposed research on selecting suitable actions under workload skew.

Completed work. The pipeline coordinates producers and consumers, freezes each run’s configuration, records partition ownership and intervention timing, and saves durable message evidence alongside Prometheus monitoring. Offline evaluation follows distinct acknowledged messages through a fixed drain cutoff. Source history, reviewed configurations, run summaries and evidence checksums are versioned; raw measurements are retained separately.

Measured results. The original 16 trials covered balanced pressure, shorter pressure, concentrated partition input and lower input. In two ten-minute balanced-pressure comparisons, keeping three consumers left 11.38% and 9.62% unfinished; scaling to six completed all evaluation messages. Recorded completion p99 decreased from 266–292 seconds to 79–96 seconds, with higher requested resources.

Eight later trials checked matching initial partition ownership, original pods and resources. Each used 1,500 messages/s, five minutes of production including one minute of warm-up, and two minutes of drain. The two pairs per workload reversed treatment order while retaining the same seed. Arrows below compare keeping three consumers with scheduling six.

| Workload and pair | Unfinished % | Completion p99 s |
| --- | --- | --- |
| 80/20 over 12   Pair 1 | 0.532 → 0.000 | 121.15 → 49.37 |
| 80/20 over 12   Pair 2 | 0.000 → 0.000 | 115.68 → 50.95 |
| 80% to P0   Pair 1 | 39.495 → 25.103 | 228.44 → 192.79 |
| 80% to P0   Pair 2 | 39.163 → 61.294 | 228.76 → 294.13 |

P99 includes only messages completed by drain; unfinished percentages include all 360,000 evaluation admissions in each run. Completion is measured after application work and before commit acknowledgment. These are explicit partition-routing tests, not original-hot-key or splitting tests.

Interpretation and limits. The 80/20 latency benefit repeated, while the single-partition result was mixed. New-consumer placement, startup delay and shared-node contention remained variable. Two pairs with synthetic CPU work provide preliminary evidence, not statistical significance, a validated application SLA, or proof that my proposed policy outperforms prior methods.

Next research stage. Hot-key splitting, targeted reassignment and the complete decision policy remain to be implemented and evaluated. The planned comparison will test no intervention, scaling alone, splitting alone and their combination with equivalent workloads, verified output correctness and explicit resource costs. The research question is whether better action selection improves outcomes beyond established scaling, assignment and routing methods.

Evidence: github.com/sjafari2/RealTime-Streaming-Pipeline; experiment-records/campaign-20260912 and experiment-records/repetitions-20260913.
