import os
import time
import shutil
import pandas as pd
from datetime import datetime
import argparse

class TimeBasedMerger:
    def __init__(self, watch_dir, processed_dir, merged_dir, min_files, interval_sec):
        self.watch_dir = watch_dir
        self.processed_dir = processed_dir
        self.merged_dir = merged_dir
        self.min_files = min_files
        self.interval = interval_sec

        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)

    def get_unprocessed_files(self):
        return [f for f in os.listdir(self.watch_dir) if f.endswith(".csv")]

    def move_to_processed(self, filepath):
        filename = os.path.basename(filepath)
        dest = os.path.join(self.processed_dir, filename)
        shutil.move(filepath, dest)

    def merge_csv_files(self, files):
        dfs = []
        for f in files:
            path = os.path.join(self.watch_dir, f)
            df = pd.read_csv(path)
            dfs.append(df)

        merged_df = pd.concat(dfs, ignore_index=True)
        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        merged_filename = f"merged_{now_str}.csv"
        merged_path = os.path.join(self.merged_dir, merged_filename)
        merged_df.to_csv(merged_path, index=False)
        print(f"Merged {len(files)} files into {merged_filename}")
        return merged_path

    def run(self):
        while True:
            unprocessed_files = self.get_unprocessed_files()
            if len(unprocessed_files) >= self.min_files:
                print(f"Found {len(unprocessed_files)} files, merging...")
                merged_path = self.merge_csv_files(unprocessed_files)
                for f in unprocessed_files:
                    self.move_to_processed(os.path.join(self.watch_dir, f))
            else:
                print(f"Waiting... only {len(unprocessed_files)} files found (need {self.min_files})")
            time.sleep(self.interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge CSVs based on time or count")
    parser.add_argument("--watchDir", required=True, help="Directory to watch for CSVs")
    parser.add_argument("--processedDir", required=True, help="Where to move processed CSVs")
    parser.add_argument("--mergedDir", required=True, help="Where to store merged CSVs")
    parser.add_argument("--minFiles", type=int, default=4, help="Minimum files to trigger merge")
    parser.add_argument("--intervalSec", type=int, default=10, help="Polling interval in seconds")
    args = parser.parse_args()

    merger = TimeBasedMerger(
        watch_dir=args.watchDir,
        processed_dir=args.processedDir,
        merged_dir=args.mergedDir,
        min_files=args.minFiles,
        interval_sec=args.intervalSec
    )
    merger.run()

