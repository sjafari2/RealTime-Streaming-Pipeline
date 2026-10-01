# Extra appendix: measurement details and preserved plots

[Read the extra appendix](Appendix_Extra.pdf) or compile [its editable LaTeX source](Appendix_Extra.tex). The 60-page supplement includes 48 plot assets. It preserves the earlier 29 assets and adds 12 detailed ten-minute plots and seven historical scaling plots. Additional figures describe existing trials, not additional experiments.

| Material | PDF pages |
| --- | --- |
| Consumer offset example and measurement coverage | 2–3 |
| Contents | 4 |
| Five-minute evaluation trials and diagnostics | 5–38 |
| Detailed ten-minute evaluation plots | 39–51 |
| Historical 24-trial scaling campaign plots | 52–59 |

The five-minute group comprises twelve trials at 700 aggregate messages/s, with one minute of warm-up and two minutes of drain (eight minutes total). The ten-minute views concern the eight trials already summarized in the main proposal. The historical campaign used an earlier deployment and different rates and durations; its figures retain their original labels and captions. It is not pooled with the newer 700-message/s study.

No plot data were changed or filled across missing observations. Partial resource-history coverage and historical monitoring limitations remain explicit. The main experimental SLO is one second; half-second results are retained as sensitivity checks. Compact ten-minute and balanced-calibration figures remain in the main proposal.

The source is flattened for portable compilation. Its local bibliography uses an explicit reference entry; the Overleaf copy retains the project's shared bibliography configuration. Required plot files are included. `figure-sha256.json` identifies all 48 assets, and `plot-preservation-audit.json` records the completeness check. Derived figures and summaries do not replace backups of raw evidence.

The [architecture and workflow diagram](Architecture_Workflow.pdf), [TikZ source](architecture-workflow.tex) and [standalone wrapper](Architecture_Workflow.tex) remain available in this directory.

Validation: the extra appendix compiled in Overleaf with zero errors, warnings or informational layout messages, and compiled locally. Added pages were visually inspected, all plot references resolve, and all 29 previously included assets remain present. No runtime code or experiment measurements changed.
