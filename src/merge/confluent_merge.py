import os
import time
import shutil
import pandas as pd
import argparse
from datetime import datetime


class HybridMerger:
    def __init__(self, watch_dir, processed_dir, merged_dir, min_files, interval_sec):
        self.watch_dir = watch_dir
        self.processed_dir = processed_dir
        self.merged_dir = merged_dir
        self.min_files = min_files
        self.interval = interval_sec
        self.last_merge_time = time.time()

        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)

    def get_unprocessed_files(self):
        return sorted([
            f for f in os.listdir(self.watch_dir)
            if f.endswith(".csv") and not f.startswith(".tmp_")
        ])

    def move_to_processed(self, filepath):
        filename = os.path.basename(filepath)
        dest = os.path.join(self.processed_dir, filename)
        shutil.move(filepath, dest)

    def merge_csv_files(self, files):
        dfs = []
        for f in files:
            path = os.path.join(self.watch_dir, f)
            try:
                df = pd.read_csv(path)

                if 'producer_timestamp' in df.columns:
                    df['producer_timestamp'] = pd.to_numeric(df['producer_timestamp'], errors='coerce')
                else:
                    print(f"'producer_timestamp' missing in {f}, skipping file")
                    continue

                df['source_file'] = f
                dfs.append(df)
            except Exception as e:
                print(f"[WARN] Skipping {f}: {e}")

        if not dfs:
            print("[INFO] No valid data to merge.")
            return None, None

        merged_df = pd.concat(dfs, ignore_index=True)

        # Record merge timestamp as close to write as possible
        merge_ts = time.time()
        merged_df['merge_timestamp'] = merge_ts
        merged_df['merge_delay_sec'] = merged_df['merge_timestamp'] - merged_df['producer_timestamp']

        # Save to a timestamped file to avoid overwrite
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        merged_filename = f"merged_{timestamp_str}.csv"
        merged_path = os.path.join(self.merged_dir, merged_filename)

        # Explicit column order
        desired_order = [
            'index',
            'topic',
            'size_bytes',
            'producer_timestamp',
            'consumer_timestamp',
            'consumer_delay_sec',
            'application_timestamp',
            'application_delay_sec',
            'merge_timestamp',
            'merge_delay_sec',
            'source_file'
        ]
        final_columns = [col for col in desired_order if col in merged_df.columns]

        #print("Columns before save:", merged_df.columns.tolist())
        #print("Final column order:", final_columns)

        try:
            merged_df.to_csv(merged_path, index=False, columns=final_columns)
            print(f"{len(files)} files → {merged_filename}")
            print(f"Saved merged file to: {merged_path}")
            return merged_path, merge_ts
        except Exception as e:
            print(f"Failed to write merged file: {e}")
            return None, None

    def run(self):
        while True:
            unprocessed_files = self.get_unprocessed_files()
            now = time.time()

            should_merge = len(unprocessed_files) >= self.min_files or \
                           (now - self.last_merge_time >= self.interval)

            if unprocessed_files and should_merge:
                print(f"Ready to merge {len(unprocessed_files)} files")
                merged_path, merge_ts = self.merge_csv_files(unprocessed_files)
                if merged_path:
                    for f in unprocessed_files:
                        self.move_to_processed(os.path.join(self.watch_dir, f))
                    self.last_merge_time = merge_ts
            else:
                print(f"Files: {len(unprocessed_files)}, Time since last merge: {now - self.last_merge_time:.1f}s")
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hybrid CSV Merger")
    parser.add_argument("--watchDir", required=True)
    parser.add_argument("--processedDir", required=True)
    parser.add_argument("--mergedDir", required=True)
    parser.add_argument("--minFiles", type=int, default=4)
    parser.add_argument("--intervalSec", type=int, default=10)
    args = parser.parse_args()

    merger = HybridMerger(
        watch_dir=args.watchDir,
        processed_dir=args.processedDir,
        merged_dir=args.mergedDir,
        min_files=args.minFiles,
        interval_sec=args.intervalSec
    )
    merger.run()

