#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Collect Parquet files from nested subdirectories into a single destination folder.

- Recursively scans SOURCE (e.g., .../processed/) for *.parquet files.
- Skips temporary files starting with ".tmp_".
- Moves (default) or copies (--copy) files into DEST (e.g., .../2025-08-31/).
- Avoids filename collisions by appending a short hash of the relative path.
- Supports --dry-run to preview actions without changing files.

Usage examples (run inside the pod):
  python3 collect_parquet.py \
    --source /path/to/20250901_134420/consumer/consumer-sts-0_consumer-result/2025-08-31/2025-09-01/2025-09-01/2025-09-01/processed \
    --dest   /path/to/20250901_134420/consumer/consumer-sts-0_consumer-result/2025-08-31

  # Copy instead of move:
  python3 collect_parquet.py --source .../processed --dest .../2025-08-31 --copy

  # Preview only:
  python3 collect_parquet.py --source .../processed --dest .../2025-08-31 --dry-run
"""

import argparse
import hashlib
import os
import sys
import shutil
from pathlib import Path
from typing import Iterable, Tuple

def short_hash(text: str, length: int = 8) -> str:
    """Return a short stable hash for a given string (used to disambiguate filenames)."""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]

def iter_parquet_files(root: Path) -> Iterable[Path]:
    """Yield all parquet files under root, skipping temp files like '.tmp_*'."""
    for p in root.rglob("*.parquet"):
        name = p.name
        if name.startswith(".tmp_"):
            continue
        # Sometimes Spark writes hidden files like "_SUCCESS"; not parquet, but be safe:
        if name.startswith("_") and not name.endswith(".parquet"):
            continue
        if p.is_file():
            yield p

def make_unique_name(dest_dir: Path, src_file: Path, rel_path: Path) -> Path:
    """
    Build a destination filename that avoids collisions:
    - Keep original stem and suffix.
    - If a file with the same name exists, append '__{hash}' before the suffix.
    The hash is computed from the relative path, making it deterministic.
    """
    stem = src_file.stem
    suffix = src_file.suffix  # ".parquet"
    candidate = dest_dir / (stem + suffix)
    if not candidate.exists():
        return candidate

    h = short_hash(str(rel_path))
    return dest_dir / f"{stem}__{h}{suffix}"

def move_or_copy(src: Path, dst: Path, do_copy: bool, dry_run: bool) -> None:
    """Move or copy the file, honoring dry-run."""
    action = "COPY" if do_copy else "MOVE"
    print(f"{action}: {src} -> {dst}")
    if dry_run:
        return

    dst.parent.mkdir(parents=True, exist_ok=True)
    if do_copy:
        # Use copy2 to preserve basic metadata (mtime, etc.)
        shutil.copy2(src, dst)
    else:
        # shutil.move handles cross-device moves as copy+delete if needed
        shutil.move(src, dst)

def main() -> int:
    parser = argparse.ArgumentParser(description="Collect parquet files into one folder.")
    parser.add_argument("--source", required=True, type=str,
                        help="Path to the 'processed' directory to scan recursively.")
    parser.add_argument("--dest", required=True, type=str,
                        help="Destination directory (e.g., .../2025-08-31).")
    parser.add_argument("--copy", action="store_true",
                        help="Copy files instead of moving them.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes.")
    args = parser.parse_args()

    src_root = Path(args.source).resolve()
    dest_dir = Path(args.dest).resolve()

    if not src_root.exists():
        print(f"ERROR: --source does not exist: {src_root}", file=sys.stderr)
        return 2
    if not src_root.is_dir():
        print(f"ERROR: --source is not a directory: {src_root}", file=sys.stderr)
        return 2

    # Create destination (even in dry-run so user can see the intended path exists)
    if not args.dry_run:
        dest_dir.mkdir(parents=True, exist_ok=True)

    # Safety check: allow moving from a subdir to an ancestor (your case), but warn if dest is inside source
    try:
        # If dest lies inside source, shutil.move will still work, but we warn to avoid surprises.
        dest_inside_source = str(dest_dir).startswith(str(src_root))
    except Exception:
        dest_inside_source = False

    if dest_inside_source:
        print(f"WARNING: Destination is inside source:\n  source: {src_root}\n  dest:   {dest_dir}\n"
              f"This is allowed, but be sure this is what you want.\n", file=sys.stderr)

    count = 0
    skipped = 0
    errors = 0

    for f in iter_parquet_files(src_root):
        # Compute relative path for stable hashing
        try:
            rel = f.relative_to(src_root)
        except Exception:
            rel = Path(f.name)

        dst = make_unique_name(dest_dir, f, rel)

        # If the destination already has an identical-sized file, skip to save time
        if dst.exists():
            try:
                if f.stat().st_size == dst.stat().st_size:
                    print(f"SKIP (exists same size): {dst}")
                    skipped += 1
                    continue
            except Exception:
                # If stat fails, fall back to creating a unique hashed name
                dst = make_unique_name(dest_dir, f, rel)

        try:
            move_or_copy(f, dst, do_copy=args.copy, dry_run=args.dry_run)
            count += 1
        except Exception as e:
            errors += 1
            print(f"ERROR moving/copying {f} -> {dst}: {e}", file=sys.stderr)

    print("\nDone.")
    print(f"  Total found/moved: {count}")
    print(f"  Skipped (already present same size): {skipped}")
    print(f"  Errors: {errors}")
    return 0 if errors == 0 else 1

if __name__ == "__main__":
    sys.exit(main())

