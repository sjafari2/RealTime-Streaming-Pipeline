# Git history and backups

The active code is in `/Users/soheila/Desktop/Thesis-26-27/code`. It continues the
history of `sjafari2/RealTime-Streaming-Pipeline`; it is not a new unrelated repository.
The active implementation and managed measurement updates are on `main`.
Earlier implementations remain preserved in Git history and the documented backups.

## What I keep in Git

- Source, tests, infrastructure definitions and documentation.
- Reviewed experiment configurations and the reason for each configuration.
- Small run records under `experiment-records/`: summaries, frozen configuration,
  evidence hashes, limitations and the code revision when it is known.

The large `results/` directory stays outside Git. It contains the individual-message
evidence, full Prometheus exports and analysis outputs. A summary in Git does not
back up those files. Keep the original evidence on Nautilus and in a separately
verified data archive before removing any copy. No cloud destination is configured
yet. Historical data already present in the old repository remains in its history
and, where retained as files, in the source archive.

## Save a completed change

After applying and checking a coherent change, make a descriptive commit. Stage the
specific files being changed so another unfinished edit is not included by accident:

```bash
cd /Users/soheila/Desktop/Thesis-26-27/code
git status --short
git add -- path/to/changed-file path/to/another-file
git diff --cached --stat
git diff --cached --check
git commit -m "Describe the completed change"
git push
```

The two paths above are placeholders for the actual changed files. A commit is local;
`git push` copies committed history to GitHub. If pushing fails, keep the local commit
and report the failure. Do not reset, amend, rebase published history, or force-push to
make a failed push appear successful. Use an ordinary follow-up commit for corrections.

Before an experiment, finish and commit the code/configuration being tested. Record
`git rev-parse HEAD` and any remaining changes with the run. The runtime also records
application source hashes and package versions in its evidence. A previously completed
run must not be assigned a newer commit as though that commit existed when it ran.

## Make a portable history backup

Use a new destination for each milestone. This preserves branches and tags without
changing the remote:

```bash
cd /Users/soheila/Desktop/Thesis-26-27/code
git fetch origin --tags
backup_dir="../code-backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_dir"
git bundle create "$backup_dir/repository.bundle" --all
git bundle verify "$backup_dir/repository.bundle"
shasum -a 256 "$backup_dir/repository.bundle" > "$backup_dir/repository.bundle.sha256"
```

A bundle backs up reachable Git history. It does not include uncommitted files,
ignored experiment data, GitHub issues, pull-request conversations, permissions or
repository settings. Commit completed work first; preserve unfinished files separately.
For a complete remote snapshot of every advertised branch/ref, use a fresh mirror
clone and create the bundle from it, as done before this integration.

Restore into a new directory, then inspect it:

```bash
git clone /absolute/path/to/repository.bundle /absolute/path/to/restored-code
git -C /absolute/path/to/restored-code log --oneline -10
```

Choose the required branch explicitly when restoring. A clone made from a bundle has
the bundle as its origin; set the GitHub origin again only when the restored checkout
is ready to use. Keep an off-machine copy of important bundles: another folder on this
Mac protects against accidental edits, but not loss of the Mac.

## Initial migration backup

The backup is outside the active code folder, under:

`/Users/soheila/Documents/ChatGPT/Stream Processing Project/review/github-integration-20260912/`

- `RealTime-Streaming-Pipeline-original.git`: mirror taken before the update.
- `RealTime-Streaming-Pipeline-before-update.bundle`: verified portable Git backup.
- `repository-backup-manifest.json`: original commit, refs and bundle checksum.
- `code-before-git-integration.tar.gz`: the pre-integration local source files,
  including existing local source backups; excludes `results/` and `.git/`.
- `code-backup-manifest.json`: source archive scope and verified file checksums.

The original `main` commit is `c59a6a90fb8fddaf430e4b850caf47892de95f92` (111 commits).
The repository's earlier Pascal-G code and descriptions remain available in that
history. Material absent from the current workflow is explained under
`archive/repository-before-managed-runs/ARCHIVE.md`.

Git's bundle format and limitations: https://git-scm.com/docs/git-bundle
