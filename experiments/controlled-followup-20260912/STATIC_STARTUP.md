# Static-member startup and the proposed controlled pair

Status: implemented for local validation; awaiting user confirmation before deployment or any Nautilus preparation/experiment. The six earlier rejected preparations are a separate, completed record. This revised procedure has not been live validated.

The consumer has an optional `CONSUMER_STATIC_MEMBERSHIP` setting. Its default is false. When true, each pod supplies its own hostname as Kafka `group.instance.id`; the shared ConfigMap must not give every replica the same identity. The client continues using classic cooperative-sticky subscription and automatic partition assignment. Adding stable identities does not guarantee any particular assignment.

A new controlled block uses one unique consumer group for all its stages. Before each fresh-topic start it stops applications and waits until Kafka reports no members and group state EMPTY or DEAD. This avoids treating departed static members as part of the next starting population. It does not remove group members through an administrative mutation or change Kafka timeouts. The deployed client/broker behavior still requires live validation.

## Sequence for review

1. Start three consumers with empty topics. Once readiness is stable, capture the actual full ownership map against the already frozen original pod identities and resources. Stop and verify complete evidence with no enqueued, acknowledged or completed messages.
2. Restart three consumers with new empty topics. Require the same complete map and original pod placement, then stop and verify empty evidence again.
3. Prepare six consumers with new empty topics. Preserve original P0-P2/C0-C2 identities, validate all six distinct member identities and capture the temporary six-consumer ownership. This is a stopped preparation; it does not measure a running-workload scaling transition.
4. Return to three consumers after the removed members clear. A new empty-topic start must reproduce the original three-consumer reference. The six-consumer preparation never replaces that reference.
5. Only when the comparison is explicitly requested and all four preparations pass, run the scale trial: three consumers initially, six requested 120 seconds after production starts.
6. Run the keep-three trial with the same starting reference and workload seed. Both live trials independently recheck before production and before the scheduled decision.

Any failed check stops the whole sequence. There are no automatic retries and no reference changes based on outcomes. The maximum is four empty preparations plus two performance trials, with a 1,200-second cumulative preparation budget. Empty preparations include cleanup/collection; admitted production, drain, evidence transfer and analysis are outside that counter. Original YAML, HPA settings and replica counts are restored and verified on completion or failure. Shared-machine contention remains a limitation even when starting pods match.

The workload remains 3 producers, 60 partitions, 1,500 target messages/s total, 80% aimed at partition 0, 100-byte payloads, 2,000 SHA-256 iterations per message and no artificial sleep. Each performance trial has 300 seconds production including 60 seconds warm-up, then 120 seconds drain. Completion is measured before commit acknowledgment; whole-run latency remains conditional on completion and is reported with unfinished outcomes. Static membership is enabled in both treatments, so this is a separate comparison from the older dynamic-membership trials.

## Commands after user confirmation

No command below has been executed for this revision. First synchronize the changed consumer code while applications are stopped, using `my-shell/sync-code.sh --role consumer`. The run preflight must verify the final source hashes in every relevant pod. In this Codex session, use the project workspace as the working directory and address the final script by absolute path; earlier cluster inspection timed out from the Desktop code directory.

Preparation only is the default for the new protocol:

```bash
cd '/Users/soheila/Documents/ChatGPT/Stream Processing Project'
python3 '/Users/soheila/Desktop/Thesis-26-27/code/experiments/controlled-followup-20260912/execute_block.py' \
  --static-startup \
  --audit-dir '/Users/soheila/Desktop/Thesis-26-27/code/results/static-startup-NEW_UNIQUE_NAME'
```

To perform all four preparations and then the two conditional performance trials in one block, add `--include-comparison`. Choose a new audit directory. A separate invocation captures a new reference; do not claim it continues an earlier reference or starts directly at the live pair. Without `--static-startup`, the script retains the older fixed-reference procedure for reproducibility; that is not the revised plan.

## Code and evidence

- `src/consumer/consumer.py` derives and reports each static identity in client configuration and runtime status.
- `my-shell/placement_control.py` captures observed ownership, checks full partition coverage and checks original pod identity/resources plus static IDs.
- `my-shell/run_experiment.py` restricts capture to preparation-only mode, enforces the common production barrier and records the checks.
- `my-shell/static_startup.py` runs the four preparation stages, checks Kafka group clearance and audits zero-message evidence before any optional comparison.
- `experiments/controlled-followup-20260912/execute_block.py` owns the shared settings, HPA pause, limits and restoration.

The block directory retains each stage's ID/status, group-clearance observations, original pod records, the frozen three-consumer reference, the separate temporary six-consumer reference and restoration records. Each result folder retains its manifest, placement journal, application evidence and preparation evidence check. Admitted performance runs also retain the existing outcome, lag and resource analyses. Failed preparation evidence is never counted as performance data.

Official background: [Confluent static membership and cooperative rebalancing](https://docs.confluent.io/platform/current/clients/consumer.html) and [librdkafka configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).
