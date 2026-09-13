# Proposal progress update — 13 September 2026

The proposal now reports **24 completed performance trials**: the original 16-trial campaign and eight later trials with verified matching starts. This documentation update adds no experiments.

The 80/20 comparison lowered recorded completion p99 with scaling in both pairs. The single-partition comparison improved in the first pair and worsened in the second. Both outcomes are retained. Hot-key splitting is a core planned research action; it has not yet been implemented or evaluated.

## Files to use

- [Active Overleaf proposal](https://www.overleaf.com/project/6aa3337d078205869d3c5fe5): Section 5 starts on page 24; Table 7 and Figure 7 are on page 33 of the checked 61-page compile.
- [One-page recommender summary — PDF](Research_Progress_for_Recommenders.pdf), [editable Word](Research_Progress_for_Recommenders.docx), and [readable text](Research_Progress_for_Recommenders.md).
- [Comparison figure — PDF](preliminary-repeated-skew.pdf), [PNG](preliminary-repeated-skew.png), and [SVG](preliminary-repeated-skew.svg).
- [Updated preliminary-results source](Preliminary_Validation_Results.tex) and [related-work results-paragraph change](main-results-paragraph.patch).
- [Underlying eight-trial record](../repetitions-20260913/README.md) and [original campaign](../campaign-20260912/campaign-report.md).

The corrected local Word proposal is at `/Users/soheila/Documents/ChatGPT/Stream Processing Project/review/metric-alignment/Proposal_2026_Metrics_Aligned.docx`, with its PDF beside it. Its results start on page 19 and its comparison figure is on page 20. The Word copy retains its existing document structure; it is not a complete reproduction of the current Overleaf literature review.

The local LaTeX mirror is `/Users/soheila/Documents/ChatGPT/Proposal/output/Thesis/overleaf/`. Its `main.pdf` is compiled locally from the reconciled source, not downloaded from Overleaf.

## Interpretation and verification

Completion occurs after application work and before commit acknowledgment. P99 uses completed evaluation-cohort messages followed through drain; unfinished percentages use all acknowledged evaluation admissions. These are whole-run results, separate from rolling monitoring. Both later pairs retain seed 71, with reversed treatment order.

Initial ownership, original pods/nodes and requested resources matched in the later trials. Placement of newly added consumers, startup delays and shared-node contention remained variable. The second 80/20 scaling resource integral covers 95.83% of the horizon. Lag gaps and the 90% coverage screen remain explicit. No statistical-significance, validated 99 ms SLA, hot-key-splitting performance, or policy-superiority claim is made.

Overleaf saved-source hashes matched the reviewed sources after reload. The online compile reported 61 pages, zero errors and zero warnings, with one typesetting notice. Changed LaTeX pages and all 37 rendered Word pages were visually checked. All 30 original Word math expressions were preserved. The one-page summary was rendered and checked. The eight later admission/completion/unfinished counts and displayed rounding were checked against the versioned JSON. No runtime source changed, and no cluster performance tests were run for this documentation update.

Evidence was reviewed at revision `aa54f977c1a0de955b3b37a9eab928acd010795c`; the new documentation commit must not be presented as the revision used by earlier experiments. `artifact-manifest.json` records delivered-file hashes. Original documents and local source backups remain outside the active runtime, in `/Users/soheila/Documents/ChatGPT/Stream Processing Project/review/proposal-results-20260913/before/`. Raw evidence remains in its existing separately retained storage; this small Git record is not a backup of that raw data.
