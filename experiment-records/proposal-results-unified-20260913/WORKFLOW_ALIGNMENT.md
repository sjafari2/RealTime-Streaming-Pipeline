# Experiment workflow wording alignment

Sections 3.1 and 3.2 now describe the implemented workflow: shell wrappers support deployment and launch configurable experiments; the Python coordinator freezes configuration, verifies readiness, sets common phase boundaries, collects run evidence and Prometheus measurements, and calls analysis scripts. Repeated-run commands retain individual results and summarize completed runs.

Checked against `my-shell/save-run.sh`, `my-shell/run_pipeline.sh` and `my-shell/run_experiment.py`. No runtime source, deployment or experiment changed.

The Proposal task applied the corresponding online architecture/workflow wording and verified saved source with a 60-page compile, zero errors and zero warnings. Its separate introduction edit was outside this task's change. This task updated only three corresponding Word workflow passages and the local PDF. All 57 equations were preserved. The 41-page render changed only pages 6–12; those pages were visually checked, and other page images are identical to the previous reviewed render.

Word SHA256: `7a3aafcee212659ea59a436dabb149f2e1f45fb519f30e3a052562685c63787d`

PDF SHA256: `f5b46d08dc1cb4aee9c0a70d3e453b0bc7a2f104f9a26e59e126d4f52bc97240`
