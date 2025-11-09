import os

def list_parquet_files(base_dir: str):
    """
    Recursively search for all parquet files under `base_dir`
    and print their paths.
    """
    if not os.path.isdir(base_dir):
        print(f"Base directory does not exist: {base_dir}")
        return []

    parquet_files = []
    for root, _, files in os.walk(base_dir):
        for f in files:
            if f.endswith(".parquet") and not f.startswith(".tmp_"):
                full_path = os.path.join(root, f)
                parquet_files.append(full_path)

    if parquet_files:
        print("Found parquet files:")
        for f in parquet_files:
            print(f)
    else:
        print("No parquet files found under", base_dir)

    return parquet_files


if __name__ == "__main__":
    base_path = "consumer-result/processed"
    list_parquet_files(base_path)

