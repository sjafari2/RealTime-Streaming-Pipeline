# Decision-rule audit: CC-MWF and latency-aware bin packing

This audit tests specific published rules before treating a limitation as a research contribution. All numerical examples here are constructed local cases. They are not Nautilus trials, are not part of the proposal's trial count, and do not reproduce the papers' reported performance tables.

The audit found two reproducible problems in the linked CC-MWF implementation and an avoidable deadline regression in a reconstruction of Ezzeddine's 2024 decision logic. Small corrections resolve these examples. Ezzeddine's existing rule already addresses the CC-MWF backlog-concentration example. Consequently, these findings do not establish a novel controller or superiority over both approaches.

## Sources and verification boundary

- Tadevosyan, Areias and Barbosa, *Towards Heterogeneous Consumer Group Autoscaling for Distributed Event Queues* (2026), Sections III-A through III-C and Algorithms 1-2. [Author publication page](https://www.dcc.fc.up.pt/~miguel-areias/?page=pubs_2026-INTEL4EC).
- The paper links to [MBrokerBench, originalVirtual](https://github.com/itsdikey/MBrokerBench/tree/originalVirtual). The audit pins revision `bed4c83dea8b68cb665cc222e080ea7736818606`. Its original C# assignment, provisioning and queue classes were compiled and executed locally. Only logging and telemetry were replaced with no-op stubs. The test provides fully warmed consumers and deterministic starting conditions. This pinned public revision is not independently certified as the revision used for the published tables.
- Ezzeddine, Baude and Huet, *Tail-Latency Aware and Resource-Efficient Bin Pack Autoscaling for Distributed Event Queues* (CLOSER 2024), Equations 4, 6 and 7, Algorithms 1-2 and Procedures 1-2, printed pages 55-57. [Publisher paper](https://www.scitepress.org/Papers/2024/126301/126301.pdf).
- The full 2024 paper was checked. The publisher's 2025 journal extension, *Latency Aware and Resource-Efficient Bin Pack Autoscaling for Distributed Event Queues: Parameters Impact and Setting*, was accessible only as a preview. Its full decision rules and qualifications remain unverified. **Do not attribute the 2024 counterexample to the 2025 paper without checking its full text.** [2025 article](https://doi.org/10.1007/s42979-025-03740-9).

The 2024 paper omits the Least-Loaded helper's pseudocode and cites an earlier heuristic. The audit reconstructs the stated two-dimensional constraints and action branches, checks every item ordering in the examples, and uses exhaustive assignment checks where appropriate. It does not execute Ezzeddine's own source. The CC-MWF repository contains a third-party Ezzeddine implementation, but it projects rebalance lag during the initial trigger check and its group wrapper rebalances before checking the action. Those differ from the published two-stage rule, so that implementation was not used as the authoritative baseline.

## What the rules actually do

CC-MWF's paper forms a required rate from production plus a dampened backlog term, selects a low-cost fleet, and prefers retaining existing owners. Its linked implementation uses an aggregate lag threshold for provisioning, retains partitions using production alone up to full nominal capacity, and also places orphaned partitions using production rather than backlog-clearing demand. The paper's Equation 4, in contrast, constrains each consumer's sum of required rates by its safety-adjusted capacity. The implementation's orphan-placement thresholds also differ from Algorithm 2's safe-capacity ordering. These differences matter when reporting a limitation of the implementation versus a limitation of the mathematical formulation.

Ezzeddine's 2024 rule checks per-consumer lag and arrival rate, uses different up/down capacity factors, and may reassign without adding consumers. Once an action is indicated, it adds arrivals expected during the blocking rebalance and repeats packing. It already considers backlog, reassignment and rebalancing overhead. Its latency target is not a promise to empty all queues within that duration under continuing arrivals.

## Case A: aggregate capacity is not a feasible partition assignment

Use the published consumer profiles: capacities 500, 1,000 and 2,500 messages/s with respective cost rates 0.5, 1 and 2.5, and safety factor 5/6. Three partitions receive 1,100, 50 and 50 messages/s, with no initial backlog.

The original C# provisioning method selects one Small and one Medium consumer. Their aggregate capacity is 1,500, but neither can process the 1,100-message/s partition. The assignment's overload fallback places it on the Medium consumer. The original queue classes accumulate exactly 1,000 messages in ten seconds with that assignment. This is a transient-state counterexample to the decision's feasibility, not a claim that the complete controller can never add a larger consumer later.

An exhaustive feasibility check rejects that fleet and selects one Large consumer. All three partitions fit within its safety-adjusted capacity. Its cost rate is 2.5 rather than 1.5: the correction needs a more expensive feasible profile. This is a familiar indivisible-item packing issue, not a new research contribution. A homogeneous Ezzeddine baseline configured with the Large capacity can handle it; a baseline restricted to capacity 1,000 cannot, which would be a different resource constraint.

## Case B: spare capacity remains unused while existing backlog clears slowly

Three fully warmed Large consumers are available. Three partitions each receive 790 messages/s and each hold 10,000 messages; all three belong to C0. Two other partitions each receive 250 messages/s with no backlog and belong to C1. C2 is idle. Input continues at 2,870 messages/s.

The original CC-MWF code selects the same three-Large fleet and retains the assignment because C0's production rate, 2,370, fits its capacity of 2,500. Its required rate including backlog is 5,370, which violates the paper's per-consumer capacity constraint. After ten seconds, the original queue code still has 28,700 messages pending.

A feasibility repair distributes the three high-backlog partitions across all three consumers and moves one low-rate partition. The resulting owner vector is `[0, 1, 2, 0, 1]`; three partitions move. This meets the paper's required-rate constraint in the same fleet. Under a separate constant-rate fluid calculation, even charging a five-second global processing pause, time to zero backlog decreases from 230.77 to 15.41 seconds. These are analytical predictions for fixed assignments, not measured Kafka recovery times. They do not use the proposal's 100-offset/30-second recovery definition.

Ezzeddine's reconstructed 2024 rule already requests reassignment here. Across all 120 item orders, its predicted fixed-assignment recovery is 15.41-18.60 seconds with the same five-second pause. A correction to CC-MWF on this case therefore does not demonstrate an improvement over Ezzeddine.

## Case C: a feasible destination assignment can still make deadlines worse

Two consumers each process 2,500 messages/s. Two partitions, both initially on C0, hold 18,000 and 3,000 messages. C1 is idle. Both partitions continue receiving 100 messages/s. Set the latency target to ten seconds, the blocking rebalance to five seconds, and the up/down factors to 0.8/0.4. The initial cohort is a deliberately constructed burst created at decision time; it is not a measured Kafka burst.

The reconstructed published rule requests reassignment for both possible item orders. Initial total lag, 21,000, exceeds the current consumer's 20,000-message threshold. After the prospective pause, projected partition lags are 18,500 and 3,500, each fitting the destination threshold. The rule therefore still accepts reassignment.

However, a feasible FIFO schedule on the unchanged consumer completes the initial cohort within 8.4 seconds. Even the stronger fluid requirement of emptying the queue under continuing input takes only 21,000/(2,500-200) = 9.13 seconds. After reassignment, the 18,000-message partition cannot complete its initial cohort before 5 + 18,000/2,500 = 12.2 seconds, even with no other work delaying it. An explicit initial-cohort calculation gives:

| Decision | Initial-cohort p99 | Initial-cohort deadline misses |
| --- | ---: | ---: |
| Keep current assignment | 8.316 s | 0 of 21,000 |
| Published 2024 action rule | 12.116 s | 5,500 of 21,000 (26.19%) |
| Add an elapsed-time admission check | 8.316 s | 0 of 21,000 |

The proposed correction in this test rejects a transition that changes a feasible initial-cohort deadline into an infeasible one. It uses known creation times, capacities and pause duration for this constructed state. It is not a fitted or validated runtime predictor. The original paper does not promise zero violations in all circumstances; the result demonstrates a particular avoidable regression under its stated blocking model, not a refutation of its whole evaluation.

Pause sensitivity is important. At zero pause, reassignment improves initial-cohort p99 to 7.116 seconds. For this state, the first deadline misses appear when the pause exceeds 2.8 seconds. A rule that always avoids moving partitions would also be wrong. Waiting time of individual records and time to empty a continuously fed queue remain different quantities.

## Recommended research decision

Do not yet claim a new drain-time-aware assignment algorithm. The CC-MWF examples justify feasibility checks, but Ezzeddine already handles the central backlog-concentration case. The elapsed-time example motivates an action-admission study: whether an intervention's benefit for waiting messages survives its actual disruption and startup time. Its novelty still needs comparison with the complete 2025 paper and other transition-aware policies.

Before a larger repetition campaign, reproduce the timing case in Kafka with measured capacities and actual timestamps. Compare no action, the published rule and the same rule with an admission check. Include both short and long transition conditions, keeping workload, initial backlog and its age distribution, resources and ownership matched. Measure cohort completion latency and deadline outcomes alongside unfinished work, recovery and resource cost. Reject the candidate limitation if the published method already prevents the regression or if the model fails to predict it under controlled conditions. Only then extend the check to redistribution versus scaling with redistribution and startup uncertainty.

## Reproducing the audit

The Python checks require only the standard library and the saved C# output:

```bash
python3 experiments/decision-rule-audit/audit.py \
  --source-output experiment-records/decision-rule-audit-20261028/upstream-execution.txt \
  --output /tmp/decision-rule-audit-results.json
```

To rerun the original source, clone the public repository separately, check out the pinned revision, and use a .NET 8 SDK. No third-party source is copied into this repository. The project links to it through `Upstream`:

```bash
git clone --branch originalVirtual https://github.com/itsdikey/MBrokerBench.git /tmp/MBrokerBench-audit
git -C /tmp/MBrokerBench-audit checkout bed4c83dea8b68cb665cc222e080ea7736818606
dotnet run --project experiments/decision-rule-audit/harness/Audit.csproj \
  -p:Upstream=/tmp/MBrokerBench-audit/MBrokerBench --configuration Release \
  > /tmp/cc-mwf-audit-output.txt
python3 experiments/decision-rule-audit/audit.py \
  --source-output /tmp/cc-mwf-audit-output.txt \
  --output /tmp/decision-rule-audit-results.json
```

Seven assertion groups passed, including exhaustive item-order checks, a healthy no-action case, an infeasible oversized partition and pause sensitivity. The audit is limited to these small deterministic states. No complete controller campaign or live experiment has been run for this comparison.
