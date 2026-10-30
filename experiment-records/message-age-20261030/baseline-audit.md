# Message age and the published decision rule

This check uses Ezzeddine et al., *Latency Aware and Resource-Efficient Bin Pack Autoscaling for Distributed Event Queues: Parameters Impact and Setting* (2025), https://doi.org/10.1007/s42979-025-03740-9.

The implementation follows the paper's two-stage decision structure. It is a reconstruction, not a certified copy of the authors' experimental release. The linked repository contains several variants with different branches and prospective-lag calculations. The `no_elastic` source inspected previously is pinned at `49331239a9c0acf362498d13c404bb0b7c88556a`.

| Published element | Location | Reconstruction |
| --- | --- | --- |
| Per-replica processing rate, averaged across replicas | Equations 1–2, page 6 | Pooled task-processing rate; the pilot records each replica's measured rate separately. |
| Assigned arrival rate and lag | Equations 3–4, page 6 | Sum the input rates and queue counts of assigned partitions. |
| Lag and rate packing limits | Equation 5, pages 6–7 | Check both dimensions with the configured safety factor. |
| Upscale, downscale, reassign, or no action | Algorithm 1 and `scaleNeeded`, page 8 | Preserve the published branch order. |
| Extra lag during rebalancing | Equations 7–8, page 7 | Add each partition's rate multiplied by the assumed interruption. |
| Action-specific second packing pass | `doScale`, page 9 | Repeat packing with prospective lag; do not force the baseline to choose six. |
| Exact least-loaded tie order | Omitted from the paper | Descending rate, deterministic partition tie order and least assigned-rate bin. This remains an explicit reconstruction choice. |

The offline test retains zero, one, three and five seconds of interruption, two cross-partition service disciplines and three initial message ages. The live pilot prepares all workers before measurement and uses a zero transition-delay model. It therefore cannot validate scale-out activation or handover cost.

The paper describes clearing assigned lag within the latency horizon as contributing to the waiting-time objective of arriving events. It also explicitly acknowledges possible violations. An old queued message exceeding its deadline is therefore not, on its own, a contradiction of the paper. The narrower question is whether the same lag/rate state can require a different response when the objective includes the end-to-end latency of messages already waiting at decision time.

The age-aware diagnostic and its age-blind ablation use the same pooled capacity and candidate assignment. Their only difference is whether each queued message's elapsed age is included in predicted completion latency. Both assume oldest-message-first service across the assigned partitions. Actual Kafka batch order is logged and may invalidate that assumption. The offline best-candidate calculation is an oracle over known outcomes, not an implemented production controller.

The live pilot uses real producer creation timestamps. It holds an identical preload for different durations, then sends the same five-second input trace while processing remains gated. It checks the resulting queue counts and the baseline decision using measured recent arrival rates. Two repetitions reverse condition and response order. Preparation failures remain separate; valid unfavorable outcomes are retained. The initial waiting cohort and continuing arrivals are reported separately.

All six consumer pods are allocated throughout this isolated probe. Active consumer count is not presented as a measured resource-cost saving. The managed pipeline configuration is not changed, and the starting replica count is restored after the pilot. These episodes are not added to the proposal's established performance-trial count.

The published rule selects an action and assignment; it does not emit the numerical p99 predictions plotted in this audit. Those predictions are separately implemented diagnostics. Both their successes and their errors must be reported without attributing their numerical forecasts to the paper.
