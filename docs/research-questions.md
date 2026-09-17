# Research questions

The project studies partition-level lag skew and mitigation in Kafka-based stream processing. These questions retain the proposal's progression from measurement to individual mechanisms and then action selection. Results are to establish when each mechanism helps, what it costs and where it fails.

## 1. Monitoring partition-level lag

**How can partition-level lag skew be characterized and monitored to identify hot partitions and determine appropriate mitigation actions?**

The initial study examines lag, backlog growth, arrival and completion rates, and partition-to-consumer assignments. A concentrated lag distribution is an observation, not proof of key-frequency skew or a particular bottleneck. Controlled workloads provide an independent basis for checking the measurements.

Current work can evaluate the monitoring pipeline. Reliable diagnosis and automatic action selection remain research questions.

## 2. Increasing the number of consumers

**How does increasing the number of consumers affect the performance of a Kafka-based stream-processing pipeline under balanced and skewed workloads?**

The scaling comparisons use a fixed partition count and measure completed throughput, completion latency, unfinished records, backlog, and resource-time. Startup and ownership-change costs are part of the intervention. Actual admitted workloads and initial assignments are recorded because identical producer targets do not guarantee identical starting conditions.

Scheduled scale-up is implemented and has been evaluated in the preliminary comparisons documented in the experiment register. The later comparisons verify matching starting conditions; the earlier comparisons retain placement as a limitation.

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

The planned evaluation compares the decision policy with relevant assignment and scaling methods under comparable information, available actions, resource budgets, and processing semantics. The research contribution will be assessed through these comparisons.

## Evaluation sequence

The completed work addresses Questions 1 and 2 through monitoring validation and scheduled-scaling comparisons under balanced and statically skewed input. The next [stability experiments](../experiments/stability-20260914/README.md) examine whether backlog settles during sustained production. Targeted reassignment, permitted key splitting, and controller comparisons follow as separate evaluation stages.

## Closest research context

Landau et al. already coordinate consumer counts and whole-partition assignments with movement-cost considerations. Daedalus already relates workload forecasts and skew-adjusted capacity to scaling and recovery. Dhalion already links diagnosis, corrective actions and post-action evaluation. These studies inform the baseline selection and the scope of the proposed contribution.

- Landau et al., *Latency and cost-aware consumer group autoscaling in message broker systems*, JPDC, 2025. [DOI](https://doi.org/10.1016/j.jpdc.2025.105071).
- Pfister et al., *Daedalus: Self-Adaptive Horizontal Autoscaling for Resource Efficiency of Distributed Stream Processing Systems*, ICPE, 2024. [DOI](https://doi.org/10.1145/3629526.3645042).
- Floratou et al., *Dhalion: Self-Regulating Stream Processing in Heron*, PVLDB, 2017. [Paper](https://www.vldb.org/pvldb/vol10/p1825-floratou.pdf).

The extended literature review belongs with the thesis material. This page records the research direction relevant to the code; it does not claim an exhaustive survey or reproduced paper results.
