# Source history and data preservation

The active implementation is maintained on `main` in `sjafari2/RealTime-Streaming-Pipeline`. Earlier implementations and collaborative work remain available in Git history and the documented archive. Public documentation describes the research software; workstation paths and personal working preferences are maintained locally.

## Versioned material

The repository contains source, tests, infrastructure definitions, reviewed experiment configurations, and documentation. Small records under `experiment-records/` retain run summaries, frozen settings, evidence hashes, limitations, and the known execution revision. Historical records identify the code used at execution time; later documentation revisions do not change that provenance.

Large per-message logs, full monitoring exports, and generated analysis outputs are stored separately. The local `results/` directory is ignored by Git. A versioned summary is not a backup of its underlying raw evidence. The [data guide](data-and-reproducibility.md) describes the files available in a collected run and the limits of the public dataset.

## Recording research changes

A reproducible experiment records the source commit, remaining local changes, frozen configuration, package versions, and runtime source hashes. Completed changes are committed separately from unrelated work. Corrections use new commits, preserving the published history.

From the repository root:

```bash
git status --short
git rev-parse HEAD
git diff --stat
```

These commands identify the checkout and any uncommitted differences. The run manifest and evidence records retain the corresponding runtime information.

## Portable source backup

The following commands create and verify a history bundle outside the checkout:

```bash
git fetch origin --tags
backup_dir="../pipeline-backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
git bundle create "$backup_dir/repository.bundle" --all
git bundle verify "$backup_dir/repository.bundle"
shasum -a 256 "$backup_dir/repository.bundle" > "$backup_dir/repository.bundle.sha256"
```

A bundle preserves reachable Git history and refs. It excludes uncommitted files, ignored raw data, GitHub issues, wiki history, and repository settings. Those materials require separate backups. A verified copy on independent storage protects against loss of the local machine.

To inspect a restored checkout:

```bash
git clone /path/to/repository.bundle /path/to/restored-code
git -C /path/to/restored-code log --oneline -10
```

The paths above are placeholders. A restored clone initially uses the bundle as its origin.

## Historical preservation

Before the September 2026 integration, the original repository history was preserved in a mirror and verified bundle, with separate source and checksum manifests. The original `main` revision was `c59a6a90fb8fddaf430e4b850caf47892de95f92` (111 commits). Those maintainer-held backups are separate from the public dataset. The earlier PASCAL-G implementation is documented in [the repository archive](../archive/repository-before-managed-runs/ARCHIVE.md).

Reference: [Git bundle documentation](https://git-scm.com/docs/git-bundle).
