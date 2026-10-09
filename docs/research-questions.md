# Research questions

The study, **Skew Mitigation in Kafka: Combinations and Execution Order**, examines how partition backlog and available capacity inform a response to skew. A whole partition has one active consumer owner. Adding replicas can change ownership, but it cannot divide one partition's processing among those replicas.

1. **Workload diagnosis.** How can partition-level lag characteristics identify conditions in which individual mitigation methods or selected combinations are effective? The measurements include backlog magnitude, growth, skew and persistent hotspots, interpreted alongside input and processing capacity.
2. **Individual methods and combinations.** How do scaling, targeted redistribution, hot-key splitting and selected combinations affect pipeline performance? Splitting requires finer-grained routing and compatible application semantics; it is distinct from moving a whole partition.
3. **Execution order.** How does the order of combined actions affect backlog, throughput and end-to-end tail latency? Where feasible, comparisons will reach the same final replica count, assignment and routing so the path to that state can be evaluated.
4. **Intervention costs.** What resource and reconfiguration costs arise, and when are they justified by the benefit? Measurements distinguish actual CPU and memory use, requested resources over time, processing interruption, transition backlog and recovery.
5. **Decision policy.** How effectively can a heuristic policy select responses using their observed benefits and costs? Thresholds will be fixed before evaluation on separate workload schedules and compared with explicitly configured baselines.

## What the implementation currently supports

The pipeline measures these signals and supports scheduled native scaling, targeted whole-partition redistribution, and scaling with targeted redistribution. The [24 published trials](../experiment-records/README.md) establish a balanced-load reference and compare scheduled responses under two skew patterns. They provide evidence for workload diagnosis and intervention trade-offs.

Hot-key splitting, systematic execution-order comparisons and the complete heuristic controller remain planned. The current scheduled actions do not demonstrate autonomous action choice. Splitting will require validation of routing, output combination and any ordering constraints before performance comparisons.
