# Unified preliminary-results and metric audit — 13 September 2026

One results section now covers all 24 performance trials (12 paired comparisons), with one outcome table and one shared figure. A separate coverage table documents measurement quality. Initial-placement-recorded (R) and matching-start-verified (V) comparisons remain distinct; no pooling across their controls or workload families is implied. Calibration and empty preparations are excluded from the 24-trial count.

## Definition alignment

- Completion p99 is the whole-run nearest-rank percentile among distinct acknowledged evaluation admissions with a valid earliest completion by drain. Unfinished count and percentage remain alongside it. It is not a rolling 30-second histogram percentile.
- Operational lag uses consumer position; processing backlog uses the contiguous completion frontier. The displayed backlog plots and recovery diagnostic use the latter. Neither offset span is the exact unfinished evaluation-cohort count.
- Useful throughput counts distinct IDs completed inside evaluation, including eligible warm-up admissions. Completion-attempt throughput can count replay; admitted-cohort completion by drain is another population.
- Requested consumer CPU time integrates sampled consumer requests over evaluation and drain. It excludes preparation, warm-up and other cluster components. K2 scaling has 95.83% resource coverage: its observed integral is not full-horizon cost or measured CPU utilization.
- Backlog area and mean cover valid intervals only. The 90% paired screen excludes only affected backlog aggregates, not valid message outcomes. Monitoring gaps are not zero values.
- Recovery confirmation uses a 1,500-offset threshold and 20-second valid hold, with follow-up ending at production end. No observed recovery is censored. Lower-input hold confirmation is a health check, not recovery from overload.
- **Legacy field correction:** `python-scripts/compare_runs.py` subtracts the recorded `decision` timestamp when calculating `request_to_first_completion_seconds`. The proposal now calls this **decision-to-first-completion delay**. The 185.22-second K2 delay is not a pure rebalance duration. No historical evidence or runtime field was silently rewritten.
- Synthetic completion follows configured work but precedes measurement-output hashing/logging, offset storage and commit acknowledgment. Application experiments must include required prediction-output recording/combining in their completion endpoint.
- Application-task duration uses the local monotonic `processing_seconds` interval. The 1.275/3.533 ms diagnostics include warm-up, evaluation and drain completions; they do not estimate full service capacity.
- Calibration admission-count coefficient of variation uses population standard deviation across partition admission counts. It describes input balance, not backlog skew.

The corrected Word document was missing the unfinished-count definition already present online. That definition and the additional result-metric explanations were inserted. All 30 original Word math expressions were preserved; the document now contains 57 math expressions.

## Reliability and remaining limits

The report explains calibration, stopped applications, fresh run-specific topics, frozen configuration, common phase boundaries, empty-topic readiness/ownership restart checks, acknowledgment/completion reconciliation, retained unfavorable outcomes and explicit monitoring gaps. Two pairs per condition, shared-machine variability and cross-node clock uncertainty remain limitations. No statistical-significance, validated 99 ms SLA, hot-key-splitting performance or full-policy superiority claim is made.

The Proposal task received these definitions and was also informed that the current producer sends the skew remainder to the cold complement (80/20, not the early prototype's 84% illustration), still supplies no Kafka message key, and that deployment manifests now exist in the repository. That task is reconciling the surrounding prototype descriptions and opening text; this audit did not overwrite its concurrent edits.

## Validation and delivery

- The versioned summaries reconcile admissions = completed + unfinished for all 24 unique runs. All 12 paired rows and fractions were checked.
- Overleaf main, results and appendix were saved and checked through the editor. The online compilation produced 60 pages; inspected log status showed zero errors and zero warnings, with a typesetting notice. The combined figure and metric equations were visually inspected online.
- Local LaTeX compilation succeeded. Existing introduction overfull and duplicate equation hyperlink notices remain outside this change. The PDF snapshot is not a replacement for the Proposal task's later introduction edits.
- Word/PDF rendered to 41 pages. Full-document contact sheets and changed metric/results pages were visually inspected. Results are on pages 21–26; metric additions are on pages 10–12.
- No runtime source changed and no new cluster experiment ran. The previous 158-test result is historical prelaunch evidence, not a test run for this documentation edit.
- Source review started from main `f9a2aa5af1c1a960b92a668bd0f7abfd5a3a801d`; this reporting revision must not be assigned to earlier experiments.
- GitHub was visibly **Private**. The recommendation draft says records are maintained on GitHub, not publicly published. DEBS is a proposed submission target for the professor's review, not a submission or acceptance.

Raw evidence stays in its existing storage. This small documentation record and artifact hashes are not a raw-data backup.
