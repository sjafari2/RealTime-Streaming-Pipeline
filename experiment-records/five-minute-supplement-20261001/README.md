# Five-minute experiment supplement and architecture overview

The extra appendix preserves twelve earlier trials at 700 aggregate messages/s: four fixed-ownership trials and two separate four-trial intervention comparisons. Each trial uses one minute of warm-up, five minutes of evaluation and two minutes of drain. The two intervention comparisons retain their own baselines and are not pooled into a direct ranking of the interventions.

[Read the five-minute supplement](Appendix_Extra.pdf) or edit [its LaTeX source](Appendix_Extra.tex). Compile the source from this directory with a LaTeX engine; the required plot files are in `figures/`. The source is flattened into one editable file for portability.

The main proposal reports 24 other trials: twelve balanced-rate calibration trials, eight ten-minute trials comparing four responses, and four ten-minute follow-up trials using four high-traffic partitions. Together with this supplement, these selected evaluations comprise 36 trials. This is the scope of these documents, not a count of every historical pipeline experiment.

Repeated five-minute outcome, resource and lag tables are consolidated. Partial resource-history coverage and historical monitoring gaps remain explicit. Related completion-outcome and lag plots share a figure. No experimental measurement was changed or reconstructed to fill a gap. The half-second deadline remains a retrospective sensitivity check; the main experimental SLO is one second.

The [combined architecture and workflow diagram](Architecture_Workflow.pdf) distinguishes the current experimental pipeline from the planned heuristic controller. Its [TikZ source](architecture-workflow.tex) and [standalone wrapper](Architecture_Workflow.tex) are included.

The `figure-sha256.json` file identifies the exact plot assets used in the supplement. These are derived figures, not a backup of raw message records. The original run records and analysis provenance remain in the corresponding experiment-record directories.

Validation: the main proposal compiled in Overleaf without errors or warnings; the supplement and merged diagram compiled locally and were visually inspected. Source audits found no missing figure/table references or duplicate labels. Numbered metric equations and experimental values were retained.
