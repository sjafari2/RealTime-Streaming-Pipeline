import os
import shutil

def move_all_parquet(base_dir: str):
    """
    Recursively search for all parquet files under `base_dir`
    (including any nested processed/ directories) and move them
    into the base_dir itself.
    """
    # Ensure the base directory exists
    if not os.path.isdir(base_dir):
        raise ValueError(f"Base directory does not exist: {base_dir}")

    for root, _, files in os.walk(base_dir):
        for f in files:
            if f.endswith(".parquet") and not f.startswith(".tmp_"):
                src_path = os.path.join(root, f)
                dest_path = os.path.join(base_dir, f)

                # If file with same name exists, rename to avoid overwrite
                if os.path.exists(dest_path):
                    base, ext = os.path.splitext(f)
                    i = 1
                    while os.path.exists(os.path.join(base_dir, f"{base}_{i}{ext}")):
                        i += 1
                    dest_path = os.path.join(base_dir, f"{base}_{i}{ext}")

                print(f"Moving {src_path} -> {dest_path}")
                shutil.move(src_path, dest_path)

if __name__ == "__main__":
    # Adjust this path inside your pod
    base_path = "consumer-result/processed"
    move_all_parquet(base_path)

