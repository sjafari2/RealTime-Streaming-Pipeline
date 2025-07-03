import os
import time
import shutil
import pandas as pd
import argparse
from datetime import datetime
import numpy as np
from flask import Flask, jsonify
import threading
import psutil
import socket

app = Flask(__name__)
metrics = {
    "last_merge_time": 0,
    "last_batch_size": 0,
    "throughput_MBps": 0,
    "late_arrival_rate": 0,
    "out_of_order_rate": 0,
    "duplicate_rate": 0,
    "cpu_percent": 0,
    "mem_percent": 0,
    "uptime_sec": 0
}

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
        self.pod_name = socket.gethostname()
        self.start_time = time.time()

        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)
        os.makedirs(self.metrics_dir, exist_ok=True)

        print(f"[MERGE] HybridMerger started. Watching: {self.watch_dir}")

    def get_unprocessed_files(self):
        files = []
        for root, _, filenames in os.walk(self.watch_dir):
            for filename in filenames:
                if filename.endswith(".csv") and not filename.startswith(".tmp_"):
                    files.append(os.path.join(root, filename))
        if files:
            print(f"[MERGE] Detected {len(files)} unprocessed file(s).")
        return sorted(files)

    def move_to_processed(self, filepath):
        dest = os.path.join(self.processed_dir, os.path.basename(filepath))
        shutil.move(filepath, dest)
        print(f"[MERGE] Moved {os.path.basename(filepath)} to processed.")

    def compute_throughput(self, batch_size_MB, duration):
        return batch_size_MB / duration if duration > 0 else 0

    def merge_csv_files(self, files):
        dfs, total_bytes = [], 0
        for f in files:
            try:
                total_bytes += os.path.getsize(f)
                df = pd.read_csv(f)
                if not {'index', 'producer_timestamp', 'consumer_receive_timestamp'}.issubset(df.columns):
                    print(f"[SKIP] {f} missing required columns.")
                    continue
                df['producer_timestamp'] = pd.to_numeric(df['producer_timestamp'], errors='coerce')
                df['index'] = pd.to_numeric(df['index'], errors='coerce')
                df['consumer_receive_timestamp'] = pd.to_numeric(df['consumer_receive_timestamp'], errors='coerce')
                df['source_file'] = os.path.basename(f)
                dfs.append(df)
            except Exception as e:
                print(f"[ERROR] Failed to process {f}: {e}")

        if not dfs:
            print(f"[MERGE] No valid files found.")
            return None, None

        merged_df = pd.concat(dfs, ignore_index=True)

        now = time.time()
        duration = now - self.last_merge_time
        batch_MB = total_bytes / (1024 * 1024)
        throughput_MBps = self.compute_throughput(batch_MB, duration)

        merged_df['is_late'] = merged_df['producer_timestamp'] < self.previous_max_producer_timestamp
        late_arrival_rate = merged_df['is_late'].mean()

        idx = merged_df['index'].dropna().astype(int).to_numpy()
        out_of_order = np.zeros(len(idx), dtype=bool)
        out_of_order[1:] = idx[1:] < idx[:-1]
        merged_df['is_out_of_order'] = False
        merged_df.loc[merged_df['index'].dropna().index, 'is_out_of_order'] = out_of_order
        out_of_order_rate = out_of_order.mean()

        duplicate_flags = merged_df['index'].duplicated()
        merged_df['is_duplicate'] = duplicate_flags
        duplicate_rate = duplicate_flags.mean()

        merged_df['merge_timestamp'] = now
        merged_df['end_to_end_delay_ms'] = (merged_df['merge_timestamp'] - merged_df['producer_timestamp']) * 1000

        fname = f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        merged_path = os.path.join(self.merged_dir, fname)
        merged_df.to_csv(merged_path, index=False)
        print(f"[MERGE] Saved: {merged_path} ({len(merged_df)} rows)")

        metrics.update({
            "last_merge_time": now,
            "last_batch_size": len(merged_df),
            "throughput_MBps": throughput_MBps,
            "late_arrival_rate": late_arrival_rate,
            "out_of_order_rate": out_of_order_rate,
            "duplicate_rate": duplicate_rate,
            "cpu_percent": psutil.cpu_percent(interval=1),
            "mem_percent": psutil.virtual_memory().percent,
            "uptime_sec": now - self.start_time
        })

        self.previous_max_producer_timestamp = merged_df['producer_timestamp'].max()
        return merged_path, now

    def run(self):
        while True:
            try:
                files = self.get_unprocessed_files()
                now = time.time()
                if files and (len(files) >= self.min_files or now - self.last_merge_time >= self.interval):
                    merged_path, merge_ts = self.merge_csv_files(files)
                    if merged_path:
                        for f in files:
                            self.move_to_processed(f)
                        self.last_merge_time = merge_ts
                else:
                    print(f"[MERGE] Waiting... ({len(files)} files)")
                    time.sleep(2)
            except KeyboardInterrupt:
                print("[MERGE] Stopped.")
                break
            except Exception as e:
                print(f"[ERROR] {e}")
                time.sleep(2)

@app.route('/metrics', methods=['GET'])
def metrics_endpoint():
    return jsonify(metrics)

def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)

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
    threading.Thread(target=start_metrics_server, daemon=True).start()
    merger.run()

