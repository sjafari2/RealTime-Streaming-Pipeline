# Dated campaign orchestration archive

These scripts record how the September 2026 preliminary campaign was coordinated,
guarded, collected and reported. They are outside active application and import
paths. `source-index.json` maps every preserved file to its SHA-256 and the protocol
or report that references it. Earlier versions match the recorded hashes exactly;
they are retained to explain technical exclusions and corrections.

This is an audit archive, not a second normal runtime. Scripts intentionally retain
the original workspace paths, dated deadlines, unique-label checks and recovery
state assumptions. Do not run them against an unrelated or current experiment.
Use the maintained `my-shell/save-run.sh` or `my-shell/run_pipeline.sh` commands and
the runtime guide for ordinary work. The campaign protocols define the exact
workloads, seeds, trial order and guard bounds used here.

Producer, consumer, coordinator, analysis and plotting modules are maintained in
the repository and preserved in Git history. Run records retain the actual source
and environment signatures; a later analysis commit is not represented as the
application revision that ran. Full message evidence remains in `results/RUN_ID/`
and on the Nautilus PVCs. This small source archive is not a full raw-data backup.

Private research-progress correspondence and proposal editing scripts are not
part of this archive.
