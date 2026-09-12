# Research questions

The project studies partition-level lag skew and mitigation in Kafka-based stream processing. These questions retain the proposal's progression from measurement to individual mechanisms and then action selection. Results are to establish when each mechanism helps, what it costs and where it fails.

## 1. Monitoring partition-level lag

**How can partition-level lag skew be characterized and monitored to identify hot partitions and determine appropriate mitigation actions?**

The initial study examines lag, backlog growth, arrival and completion rates, and partition-to-consumer assignments. A concentrated lag distribution is an observation, not proof of key-frequency skew or a particular bottleneck. Controlled workloads provide an independent basis for checking the measurements.

Current work can evaluate the monitoring pipeline. Reliable diagnosis and automatic action selection remain research questions.

## 2. Increasing the number of consumers

**How does increasing the number of consumers affect the performance of a Kafka-based stream-processing pipeline under balanced and skewed workloads?**

With a fixed partition count, compare completed throughput, completion latency, unfinished records, backlog and resource-time. When scaling occurs during a run, include startup and ownership-change costs. Compare actual admitted workloads and initial assignments rather than assuming that identical producer targets produce identical conditions.

Scheduled scale-up is implemented. A calibrated workload and matched no-action treatment are needed before drawing a scaling conclusion.

## 3. Targeted partition reassignment

**How does targeted partition reassignment affect the performance of a Kafka-based stream-processing pipeline under skewed workloads?**

The proposed intervention moves whole busy partitions to less loaded or dedicated existing consumers. It is most relevant when several busy partitions share one consumer and spare capacity exists elsewhere. It cannot create intra-partition parallelism for a partition that already exceeds an isolated consumer's capacity.

This stage requires a supported ownership-transfer protocol, progress handover and appropriate ordering checks. Ordinary rebalancing after scale-up is not a controlled reassignment treatment.

## 4. Producer-side hot-key splitting

**How does producer-side hot-key splitting affect the performance of a Kafka-based stream-processing pipeline under skewed workloads?**

This optional stage applies to applications that allow independent record processing or correct combination of partial state. Evaluation must include routing, combination and correctness costs, together with the time needed to process records already queued under the previous routing.

Splitting a key across partitions does not preserve its original sequential processing order. It is outside the first measurement/scaling experiments.

## 5. Selecting an appropriate mitigation action

**Does selecting between mitigation actions improve latency, resource use and stability compared with existing methods and simple fixed rules?**

The planned controller will use partition-level evidence, the current assignment, capacity estimates and observed intervention costs to decide whether to wait, reassign or scale. The detailed evaluation considers action usefulness, timing, comparative effectiveness and robustness to imperfect observations.

A useful additional claim requires an advantage over competent assignment/scaling methods under comparable information, actions, resources and processing semantics. Combining existing mechanisms, using another language or adding machine learning does not alone establish novelty.

## Immediate direction

Start with Questions 1 and 2: validate balanced and statically skewed monitoring, then compare no action with scheduled scaling under a workload that produces persistent backlog and can use additional partition parallelism. This establishes evidence about measurements and action costs before attempting a complete selector. The [review-only experiment plan](../experiments/next-run-review/README.md) distinguishes a quick screening run from the longer proposal schedule.

## Closest research context

Landau et al. already coordinate consumer counts and whole-partition assignments with movement-cost considerations. Daedalus already relates workload forecasts and skew-adjusted capacity to scaling and recovery. Dhalion already links diagnosis, corrective actions and post-action evaluation. These are substantive precedents, not merely background references.

- Landau et al., *Latency and cost-aware consumer group autoscaling in message broker systems*, JPDC, 2025. [DOI](https://doi.org/10.1016/j.jpdc.2025.105071).
- Pfister et al., *Daedalus: Self-Adaptive Horizontal Autoscaling for Resource Efficiency of Distributed Stream Processing Systems*, ICPE, 2024. [DOI](https://doi.org/10.1145/3629526.3645042).
- Floratou et al., *Dhalion: Self-Regulating Stream Processing in Heron*, PVLDB, 2017. [Paper](https://www.vldb.org/pvldb/vol10/p1825-floratou.pdf).

The extended literature review belongs with the thesis material. This page records the research direction relevant to the code; it does not claim an exhaustive survey or reproduced paper results.
