# Lag availability after partition handover

The two redistribution trials on 28 September had an approximately 15-second processing pause but a much longer gap in valid total lag. The original monitoring exports and outcome records are preserved unchanged.

## Diagnosis from saved evidence

For `run-20260928-035646`, the last affected partition first completed a record 78.402 seconds after the resume request; its first valid lag sample followed at 83.824 seconds. For `run-20260928-040804`, the corresponding times were 74.838 and 76.386 seconds. Across both runs, newly assigned partitions reported invalid offset observations until they began returning records. Total lag requires a fresh, uniquely owned observation for every partition, so the last unresolved partition kept the total unavailable.

The previous consumer discarded a lag observation when `Consumer.position()` returned its unresolved sentinel, even though assignment had already resolved the exact absolute starting offset from the committed completion frontier and retained broker bounds. The saved invalid events did not include the raw offsets, so live diagnostic verification is required to confirm the sentinel mechanism. Record scheduling can delay the first return from a cold partition while hot partitions continue processing; the saved completion timing establishes that delay but does not isolate its lower-level scheduling cause.

## Measurement correction

The consumer retains the verified assignment offset until a record from that partition is returned, or until the client provides a nonnegative position. Only the unresolved `OFFSET_INVALID` sentinel can use that known starting offset. Returning a batch clears this fallback for every record in the batch before application processing begins, including records in an unprocessed suffix. Revoke/release clears it; a subsequent assignment initializes a new value.

Every observation still requires a fresh successful broker-watermark query and offsets within the retained bounds. Timeouts, expired progress, other negative offsets, unknown ownership, and stale observations remain unavailable. The correction does not change processing, commits, assignment, workload, or the freshness limit. It does not replace missing historical samples or invent zero backlog.

Two additional gauges identify the raw client position and whether the reported position came from the assignment start. Transition events record the relevant offsets and source, and invalid events retain raw values for diagnosis. These use the existing queries.

## Validation gate

Run regression tests covering unresolved startup/handoff positions, complete-batch returns, revocation, reassignment, retention/truncation, query failure, and freshness expiration. Then run the existing nonempty redistribution technical validation, separately from performance trials. Confirm raw unresolved positions with valid assignment starts, correct identity/offset continuity, and recovery of valid total lag after ownership becomes complete. Do not begin the eight requested performance trials if the cause or correction remains unverified.

The requested subsequent block has four conditions, two runs each: keep three, redistribute within three, scale to six with targeted redistribution, and scale to six with Kafka's normal rebalance. Each has 60 seconds warm-up, 600 seconds evaluation, and 120 seconds drain. Its implementation and comparability checks are pending this monitoring gate.

Reference: [Confluent Python consumer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.position).

## Live validation result

The live technical trial `run-20260929-014448` ran revision `d7db497` and confirmed the mechanism: all sixty newly assigned partitions initially returned raw client position `-1001`, while their verified assignment starts lay within freshly queried broker bounds. For example, partition 1 had starting offset 1,353 and broker high offset 3,244, so its measurable backlog span was 1,891 offsets despite the unresolved client position.

The first complete valid lag sample was 4.803 seconds after the resume request and 0.602 seconds after active ownership was verified. There were no invalid total-lag samples after active verification plus ten seconds. Overall evaluation coverage was 90%, retaining the genuine handover gap and excluding intervals across ownership changes. The raw client position took as long as 28.715 seconds to become available for a partition, but this no longer hid its known backlog. This technical trial has a shorter workload than the historical performance trials; its timing is evidence of the corrected measurement mechanism, not a performance comparison.

All 167,986 acknowledged messages completed. No duplicate message IDs, duplicate offsets, unmatched completion identities, or missing completed-prefix offsets were found. The original configuration was restored and applications stopped. The 274-test local suite passed before deployment; the new four-condition configuration tests add four passing cases. Technical validation remains separate from the 48 completed performance trials.

[Live verification and evidence hashes](live-verification.json) and [saved-run timing diagnosis](saved-run-diagnosis.json) preserve the audit. The next block is documented in the [four-condition protocol](../../experiments/four-condition-20260929/README.md).
