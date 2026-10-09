# History and backups

The project continues the existing history of `sjafari2/RealTime-Streaming-Pipeline`. `development` is the working branch; reviewed changes are merged into `main`. Corrections use new commits rather than rewriting published history.

The [archive/pre-cleanup-20261008 branch](https://github.com/sjafari2/RealTime-Streaming-Pipeline/tree/archive/pre-cleanup-20261008) preserves the repository before the current cleanup, including earlier campaigns, technical preparations and legacy source. The active branch keeps the 24 main-proposal trials. Original run IDs, execution revisions and numerical outcomes remain unchanged; archiving an earlier campaign does not invalidate or conceal its unfavorable results.

Before the cleanup, the repository was preserved in a complete Git mirror and verified bundle, and local uncommitted files were copied separately with checksum verification. These backups are outside the runtime tree.

## Creating a history backup

```bash
git fetch origin --tags
backup_dir="../pipeline-backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
git bundle create "$backup_dir/repository.bundle" --all
git bundle verify "$backup_dir/repository.bundle"
shasum -a 256 "$backup_dir/repository.bundle" > "$backup_dir/repository.bundle.sha256"
```

A bundle preserves reachable Git history and refs. It excludes uncommitted files, ignored raw results, GitHub issues, wiki history and repository settings. Those require separate backups, including a verified copy on independent storage.

```bash
git clone /path/to/repository.bundle /path/to/restored-code
```

The restored clone initially uses the bundle as its origin. Historical source revisions describe code as executed; later documentation changes do not change that provenance. The [data guide](data-and-reproducibility.md) describes the separately retained raw evidence.
