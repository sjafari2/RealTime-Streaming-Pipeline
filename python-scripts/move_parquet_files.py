import os
from typing import Tuple

# ---------- Config ----------
TOP_PROCESSED = "results/20250902_194416/consumer/consumer-sts-0_consumer-result/2025-09-02"
# Set this to your top-level "consumer-result/processed" path
# ---------------------------

def _unique_name_in_parent(fname: str) -> str:
    """
    If fname exists in the parent directory, append _1, _2, ... before extension.
    Assumes current working dir is the child; parent is os.pardir ('..').
    """
    base, ext = os.path.splitext(fname)
    candidate = fname
    i = 1
    while os.path.exists(os.path.join(os.pardir, candidate)):
        candidate = f"{base}_{i}{ext}"
        i += 1
    return candidate

def _bubble_here() -> Tuple[int, int]:
    """
    In the CURRENT directory, move all parquet files up one level (..).
    Returns (moved_count, skipped_count).
    """
    moved = 0
    skipped = 0
    with os.scandir(".") as it:
        for entry in it:
            if not entry.is_file():
                continue
            name = entry.name
            if not name.endswith(".parquet") or name.startswith(".tmp_"):
                continue

            dest_name = _unique_name_in_parent(name)
            src_rel = name
            dst_rel = os.path.join(os.pardir, dest_name)
            try:
                # Move up one level using short relative paths only
                os.replace(src_rel, dst_rel)  # atomic rename across same filesystem
                moved += 1
            except OSError as e:
                print(f"[WARN] Could not move '{src_rel}' -> '{dst_rel}': {e}")
                skipped += 1
    return moved, skipped

def _recurse_and_bubble() -> Tuple[int, int]:
    """
    Depth-first:
    - For each subdirectory, chdir into it, recurse, chdir back.
    - Then bubble current dir's parquet files one level up.
    Operates purely with short relative names, avoiding long absolute paths.
    Returns total (moved_count, skipped_count) within this subtree.
    """
    total_moved = 0
    total_skipped = 0

    # First recurse into subdirectories one-by-one using chdir
    # Collect child dir names first to avoid modifying while iterating
    subdirs = []
    with os.scandir(".") as it:
        for entry in it:
            if entry.is_dir(follow_symlinks=False):
                subdirs.append(entry.name)

    for d in subdirs:
        try:
            os.chdir(d)
        except OSError as e:
            print(f"[WARN] Cannot enter dir '{d}': {e}")
            continue

        m, s = _recurse_and_bubble()
        total_moved += m
        total_skipped += s

        # Return to parent using short relative path
        os.chdir(os.pardir)

    # Now bubble all parquet files in THIS directory one level up
    m, s = _bubble_here()
    total_moved += m
    total_skipped += s

    return total_moved, total_skipped

def main():
    # 1) Go to a short ancestor path
    if not os.path.isdir(TOP_PROCESSED):
        raise SystemExit(f"Top processed directory does not exist: {TOP_PROCESSED}")

    # 2) cd into the top processed dir (keep the path short for syscalls)
    os.chdir(TOP_PROCESSED)

    # 3) Recurse and bubble up from deepest to here
    moved, skipped = _recurse_and_bubble()
    print(f"\nDone. Parquet files moved up to: {TOP_PROCESSED}")
    print(f"Moved: {moved}, Skipped: {skipped}")

    # 4) Optional: prune empty directories (safe pass)
    pruned = 0
    # Walk bottom-up removing empty dirs; use short relative steps
    for root, dirs, files in os.walk(".", topdown=False):
        # Skip the top '.'
        if root == ".":
            continue
        if not dirs and not files:
            try:
                os.rmdir(root)
                pruned += 1
            except OSError:
                pass
    if pruned:
        print(f"Removed {pruned} empty directories.")

if __name__ == "__main__":
    main()

