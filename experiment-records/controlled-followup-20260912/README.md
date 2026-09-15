# Controlled concentrated-input follow-up: preparation outcome

On 12 September 2026, the bounded follow-up stopped after all six preparations failed the frozen partition ownership reference. No production was released, no intervention occurred, and no keep-versus-scale performance pair was obtained. These preparations do not add to the sixteen earlier performance trials.

## Purpose and planned comparison

Compare keeping three consumers with scaling from three to six, starting with the same original producer and consumer pods, machines, declared resources and complete partition ownership. This addresses the starting-owner and machine differences in the earlier concentrated-input results. It asks whether adding consumers helps when one partition receives most traffic; it does not implement hot-key splitting or targeted reassignment.

The preselected order was scale then keep, with workload seed 71. Both treatments were configured for three producers, 60 partitions, 1,500 target messages/s total, 80% of messages aimed at partition 0, 100-byte payloads and 2,000 SHA-256 iterations per message, with no artificial sleep. Planned production was 300 s including 60 s warm-up, followed by 120 s drain. Scaling was scheduled 120 s after production started. These are configured settings, not an achieved workload.

## What happened

The first successful preparation had to be a preparation-only check. Each attempt restarted the applications with fresh empty topics and waited for complete, stable readiness. Kafka allocated 20 partitions to each consumer, but the exact ownership differed from the reference. Every attempt stopped before the common production barrier was released.

The frozen map required partitions p to belong to consumer p modulo 3. The following groups contain all 60 partitions. C0 means consumer-sts-0, and likewise for C1/C2.

| Preparation | Owner of 0,3,...,57 | Owner of 1,4,...,58 | Owner of 2,5,...,59 | Admission | Acknowledged / completed |
|---|---|---|---|---|---|
| Frozen reference | C0 | C1 | C2 | Required | Not applicable |
| run-20260912-170614 | C2 | C0 | C1 | Rejected | 0 / 0 |
| run-20260912-170822 | C1 | C0 | C2 | Rejected | 0 / 0 |
| run-20260912-171032 | C2 | C1 | C0 | Rejected | 0 / 0 |
| run-20260912-171242 | C1 | C2 | C0 | Rejected | 0 / 0 |
| run-20260912-171450 | C1 | C2 | C0 | Rejected | 0 / 0 |
| run-20260912-171702 | C2 | C0 | C1 | Rejected | 0 / 0 |

All six used the same reference hash. All 36 producer/consumer final records were present, finished and free of recorded application/evidence errors. Each preparation had zero acknowledged and zero completed message events, with no production start time. The cumulative preparation time was 760.20 s (12 min 40 s), below the 900 s time cap; the six-attempt cap ended the block. Failed/preparation-only attempt time includes cleanup and collection. Initial checks, capacity checks and final restoration are outside that cumulative preparation counter.

Equal partition counts did not satisfy the complete ownership check. The reference is a validation rule, not an assignment instruction sent to Kafka. The application uses automatic classic cooperative-sticky membership without a static group.instance.id; the observations demonstrate that the fresh starts did not reproduce the required map. Static membership by itself should not be represented as a guarantee of any arbitrary chosen map.

## Validation and restoration

The updated coordinator and placement checks passed 122 local automated tests. The live attempts exercised readiness and rejection of ownership mismatches. Successful matched preparation, admission to production and the pre-action identity/assignment check remain unvalidated live. Existing producer/consumer application source was not modified for this follow-up, and cluster imports/source hashes passed preflight in every attempt.

The coordinator verified restoration of the original shared YAML, the original HPA identity and settings, and the original three producer/six consumer replica counts; all nine pods were Ready at restoration. A separate read-only runtime check found no producer.py or consumer.py application running in any of those nine pods. Supervisors remain available, while run control is failed and does not release another workload. The six original reference pods retained their identities, nodes, container/image IDs, restart counts and resources through the end of the block.

No latency, p99, unfinished percentage, throughput comparison or resource efficiency is reported for these empty preparations. With no admitted message cohort, these performance statistics are not applicable. The earlier sixteen performance trials and their limitations remain unchanged.

## Provenance and storage

- Executed coordinator commit: eb7c31d00e5daeee0b82bd70078d862e623536eb (includes placement implementation 710fac74a240b0384d58f9c7c3db115b363ebb8f).
- Frozen reference SHA-256: 5d6220b7d567ee3316962dc63262bad45007e0bd495b1468c6139d0b977c3001.
- Versioned companion records: block-status.json, block-audit.json, placement-reference.json, experiment-config.yaml, final-runtime-check.json, restoration-summary.json and evidence-archive-manifest.json.
- Original local results: /Users/soheila/Desktop/Thesis-26-27/code/results/RUN_ID, using the six IDs in the table.
- Complete coordinator/restoration records and logs: /Users/soheila/Documents/ChatGPT/Stream Processing Project/review/controlled-followup-execution-20260912/block-workspace and block-workspace.log.
- Separate verified local evidence archive: review/controlled-followup-execution-20260912/six-preparations-evidence.tar.gz (138 files; every archived member matched its SHA-256). This is not an off-machine backup. No Google Cloud destination has been configured.

Three earlier coordinator launch failures are preserved in audit directories block, block-authenticated and block-ready. They failed during initial read-only pod inspection before preparation or cluster mutation. In this execution environment, the same cluster command succeeded from the project workspace but timed out from the Desktop code directory; the underlying cause is not established. The completed bounded invocation used the absolute final script path with the workspace as its working directory. Authentication and browser inspection were separate initial access issues; no authentication protection was changed.

## Remaining step

Reproducible starting ownership still needs an explicitly defined and validated startup method before a new controlled pair. The completed block was not restarted, its reference was not changed and its attempt limit was not relaxed. A future block must declare any revised startup procedure before collecting performance outcomes. Even matched starting placement cannot remove changing shared-machine contention or determine placement of newly added consumers.
