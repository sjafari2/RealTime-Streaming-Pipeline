#!/usr/bin/env python3
import os
import re

# ==== CONFIG: set this to your TOP  dir ====
TOP_PROCESSED = "results/20250902_063935/consumer/consumer-sts-0_consumer-result/processed"
# ====================================================

# Matches a filename *base* ending with repeated identical numeric tails:
# e.g., "batch_1_1_1"  or "file_23_23_23_23"
RE_REPEAT_IDENTICAL_TAIL = re.compile(r'(_\d+)\1+$')

def collapse_repeated_tail(base: str) -> str:
    """
    Repeatedly collapse identical numeric suffix groups:
      'name_1_1_1'   -> 'name_1'
      'name_2_2'     -> 'name_2'
      'name_23_23_23'-> 'name_23'
    Stops when no more repeats remain.
    """
    while True:
        m = RE_REPEAT_IDENTICAL_TAIL.search(base)
        if not m:
            break
        # keep a single occurrence of the repeated group
        base = base[:m.start()] + m.group(1)
    return base

def unique_name_in_dir(dirpath: str, fname: str) -> str:
    """
    Ensure 'fname' is unique in 'dirpath' by appending __dedup, __dedup2, ...
    (Used only when the target name already exists.)
    """
    root, ext = os.path.splitext(fname)
    candidate = fname
    i = 1
    while os.path.exists(os.path.join(dirpath, candidate)):
        suffix = "" if i == 1 else str(i)
        candidate = f"{root}__dedup{suffix}{ext}"
        i += 1
    return candidate

def sanitize_tree(top_dir: str) -> None:
    if not os.path.isdir(top_dir):
        raise SystemExit(f"Top directory does not exist: {top_dir}")

    # Work with short paths only to avoid 'File name too long'
    os.chdir(top_dir)

    total_seen = 0
    total_renamed = 0
    total_skipped = 0

    # Walk bottom-up so renames don’t confuse os.walk
    for root, _, files in os.walk(".", topdown=False):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            total_seen += 1

            base, ext = os.path.splitext(f)
            new_base = collapse_repeated_tail(base)
            if new_base == base:
                total_skipped += 1
                continue  # nothing to change

            new_name = new_base + ext
            src = os.path.join(root, f)
            dst = os.path.join(root, new_name)

            # If destination exists, pick a unique variant
            if os.path.exists(dst):
                dirpath = root if root != "." else "."
                new_name = unique_name_in_dir(dirpath, new_name)
                dst = os.path.join(dirpath, new_name)

            try:
                # Use short relative paths; same-directory atomic replace
                os.replace(src, dst)
                print(f"renamed: {src} -> {dst}")
                total_renamed += 1
            except OSError as e:
                print(f"[WARN] rename failed: {src} -> {dst}: {e}")

    print(f"\nDone. Seen: {total_seen}, Renamed: {total_renamed}, Unchanged: {total_skipped}")

if __name__ == "__main__":
    sanitize_tree(TOP_PROCESSED)

