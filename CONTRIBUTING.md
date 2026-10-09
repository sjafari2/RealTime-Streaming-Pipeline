# Contributing

Development takes place on `development`; `main` holds reviewed changes. Small, focused commits keep changes to measurements, runtime behavior and research interpretation easy to follow.

```bash
git switch development
git pull --ff-only origin development
# Make and check the change.
python3 -m pytest -q
git add path/to/changed-file
git commit -m "Describe the change"
git push origin development
```

A pull request from `development` to `main` describes the change, the checks performed and any remaining limitations. Deployment and cluster experiments are separate from a source merge.

Comments explain the purpose of non-obvious code. Documentation addresses researchers using or extending the platform. Logs use plain text labels without emoji. Historical results retain their original run identifiers, settings and execution revisions; corrections are recorded in new commits.

For a new experiment, add a protocol under `experiments/` and a concise result summary under `experiment-records/`, with links from their indexes. Include all valid outcomes, measurement gaps and relevant limitations. Generated JSON, CSV and plots come from the analysis scripts and are not maintained by hand. Raw logs, credentials and personal working files stay outside Git.

The [archive branch](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008) preserves material removed from the active tree. It is a historical reference, not an alternative runtime. [History and backups](docs/git-history-and-backups.md) explains what Git does and does not preserve.
