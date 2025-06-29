# Final BSP-aligned `HybridMerger` script with full missing data detection
# Flags data as "missing" if any necessary fields are NaN (index, size_bytes, producer_timestamp, merge_timestamp)
# Tracks:
#   - throughput_MBps (batch-level)
#   - missing_rate_index (batch-level)
#   - missing_rate_fields (batch-level)
#   - late_arrival_rate (batch-level)
#   - out_of_order_rate (batch-level)
#   - duplicate_rate (batch-level)
# Saves:
#   - merged CSV with per-row flags (is_late, is_out_of_order, is_duplicate, is_missing_critical)
#   - merge_metrics.csv with batch-level metrics for monitoring

import os
import time
import shutil
import pandas as pd
import argparse
from datetime import datetime
import numpy as np

class HybridMerger:
    def __init__(self, watch_dir, processed_dir, merged_dir, metrics_dir, min_files, interval_sec):
        self.watch_dir = watch_dir
        self.processed_dir = processed_dir
        self.merged_dir = merged_dir
        self.metrics_dir = metrics_dir
        self.min_files = min_files
        self.interval = interval_sec
        self.last_merge_time = time.time()
        self.previous_max_producer_timestamp = 0

        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)
        os.makedirs(self.metrics_dir, exist_ok=True)

    def get_unprocessed_files(self):
        return sorted([f for f in os.listdir(self.watch_dir) if f.endswith(".csv") and not f.startswith(".tmp_")])

    def move_to_processed(self, filepath):
        shutil.move(filepath, os.path.join(self.processed_dir, os.path.basename(filepath)))

    def compute_throughput(self, batch_size_MB, superstep_duration):
        return batch_size_MB / superstep_duration if superstep_duration > 0 else 0

    def merge_csv_files(self, files):
        dfs = []
        total_bytes = 0

        for f in files:
            path = os.path.join(self.watch_dir, f)
            try:
                total_bytes += os.path.getsize(path)
                df = pd.read_csv(path)
                if 'producer_timestamp' not in df.columns or 'index' not in df.columns:
                    continue
                df['producer_timestamp'] = pd.to_numeric(df['producer_timestamp'], errors='coerce')
                df['index'] = pd.to_numeric(df['index'], errors='coerce')
                df['size_bytes'] = pd.to_numeric(df.get('size_bytes', pd.Series([None]*len(df))), errors='coerce')
                df['source_file'] = f
                dfs.append(df)
            except Exception:
                continue

        if not dfs:
            return None, None

        merged_df = pd.concat(dfs, ignore_index=True)

        merge_ts = time.time()
        superstep_duration = merge_ts - self.last_merge_time
        batch_size_MB = total_bytes / (1024 * 1024)
        throughput_MBps = self.compute_throughput(batch_size_MB, superstep_duration)

        # Late message detection
        merged_df['is_late'] = merged_df['producer_timestamp'] < self.previous_max_producer_timestamp
        late_arrival_rate = merged_df['is_late'].mean()

        # Out-of-order detection
        indices_array = merged_df['index'].dropna().astype(int).to_numpy()
        out_of_order_flags = np.zeros(len(indices_array), dtype=bool)
        out_of_order_flags[1:] = indices_array[1:] < indices_array[:-1]
        merged_df['is_out_of_order'] = False
        merged_df.loc[merged_df['index'].dropna().index, 'is_out_of_order'] = out_of_order_flags
        out_of_order_rate = out_of_order_flags.mean()

        # Duplicate detection
        duplicate_flags = merged_df['index'].duplicated()
        merged_df['is_duplicate'] = duplicate_flags
        duplicate_rate = duplicate_flags.mean()

        # Missing index rate detection
        indices_present = set(merged_df['index'].dropna().astype(int))
        if indices_present:
            expected_indices = set(range(min(indices_present), max(indices_present) + 1))
            missing_indices = expected_indices - indices_present
            missing_rate_index = len(missing_indices) / len(expected_indices)
        else:
            missing_rate_index = 1.0

        # Field-level missing data detection on critical fields
        critical_cols = ['index', 'size_bytes', 'producer_timestamp']
        missing_fields_count = merged_df[critical_cols].isnull().sum().sum()
        total_fields = merged_df[critical_cols].shape[0] * len(critical_cols)
        missing_rate_fields = missing_fields_count / total_fields if total_fields > 0 else 1.0

        # Flag rows with any critical field missing as "is_missing_critical"
        merged_df['is_missing_critical'] = merged_df[critical_cols].isnull().any(axis=1)

        # Add merge timestamp and delay columns
        merged_df['merge_timestamp'] = merge_ts
        merged_df['end_to_end_delay'] = merged_df['merge_timestamp'] - merged_df['producer_timestamp']

        # Save merged CSV
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        merged_filename = f"merged_{timestamp_str}.csv"
        merged_path = os.path.join(self.merged_dir, merged_filename)

        desired_order = [
            'index', 'topic', 'size_bytes', 'producer_timestamp', 'consumer_timestamp',
            'consumer_delay_sec', 'application_timestamp', 'application_delay_sec',
            'merge_timestamp', 'end_to_end_delay',
            'is_late', 'is_out_of_order', 'is_duplicate', 'is_missing_critical', 'source_file'
        ]

        merged_df.to_csv(merged_path, index=False, columns=[col for col in desired_order if col in merged_df.columns])

        # Save batch-level metrics
        metrics_row = {
            'timestamp': datetime.now().isoformat(),
            'batch_size': len(merged_df),
            'throughput_MBps': throughput_MBps,
            'late_arrival_rate': late_arrival_rate,
            'out_of_order_rate': out_of_order_rate,
            'duplicate_rate': duplicate_rate,
            'missing_rate_index': missing_rate_index,
            'missing_rate_fields': missing_rate_fields
        }
        metrics_df = pd.DataFrame([metrics_row])
        metrics_file = os.path.join(self.metrics_dir, "merge_metrics.csv")
        metrics_df.to_csv(metrics_file, mode='a', header=not os.path.exists(metrics_file), index=False)

        # Update state for next batch
        self.previous_max_producer_timestamp = merged_df['producer_timestamp'].max()

        return merged_path, merge_ts

    def run(self):
        while True:
            unprocessed_files = self.get_unprocessed_files()
            now = time.time()

            if unprocessed_files and (len(unprocessed_files) >= self.min_files or now - self.last_merge_time >= self.interval):
                merged_path, merge_ts = self.merge_csv_files(unprocessed_files)
                if merged_path:
                    for f in unprocessed_files:
                        self.move_to_processed(os.path.join(self.watch_dir, f))
                    self.last_merge_time = merge_ts
            else:
                time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--watchDir", required=True)
    parser.add_argument("--processedDir", required=True)
    parser.add_argument("--mergedDir", required=True)
    parser.add_argument("--metricsDir", required=True)
    parser.add_argument("--minFiles", type=int, default=4)
    parser.add_argument("--intervalSec", type=int, default=10)
    args = parser.parse_args()

    merger = HybridMerger(
        watch_dir=args.watchDir,
        processed_dir=args.processedDir,
        merged_dir=args.mergedDir,
        metrics_dir=args.metricsDir,
        min_files=args.minFiles,
        interval_sec=args.intervalSec
    )
    merger.run()

