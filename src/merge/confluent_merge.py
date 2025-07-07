import os
import time
import shutil
import pandas as pd
import argparse
from datetime import datetime
import numpy as np
import socket
import psutil
import threading
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Gauge
import signal
import sys

app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

throughput_gauge = Gauge('merge_throughput_MBps', 'Throughput in MB/s during merge')
late_rate_gauge = Gauge('merge_late_arrival_rate', 'Late arrival rate')
out_of_order_gauge = Gauge('merge_out_of_order_rate', 'Out-of-order rate')
duplicate_gauge = Gauge('merge_duplicate_rate', 'Duplicate rate')
cpu_gauge = Gauge('merge_cpu_percent', 'CPU percent usage')
mem_gauge = Gauge('merge_memory_percent', 'Memory percent usage')
uptime_gauge = Gauge('merge_uptime_seconds', 'Merger uptime in seconds')
last_merge_time_gauge = Gauge('merge_last_merge_timestamp', 'Last merge completion timestamp (Unix time)')
last_batch_size_gauge = Gauge('merge_last_batch_size', 'Number of records in last merged batch')

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
        self.running = True

        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)
        os.makedirs(self.metrics_dir, exist_ok=True)

        print(f"[MERGE] HybridMerger started. Watching: {self.watch_dir}")

        # Start system metrics updater thread
        threading.Thread(target=self.update_metrics_periodically, daemon=True).start()

    def update_metrics_periodically(self):
        while self.running:
            try:
                cpu_gauge.set(psutil.cpu_percent(interval=1))
                mem_gauge.set(psutil.virtual_memory().percent)
                uptime_gauge.set(time.time() - self.start_time)
            except Exception as e:
                print(f"[ERROR] Metrics update error: {e}")
            time.sleep(5)

    def get_unprocessed_files(self):
        files = []
        for root, _, filenames in os.walk(self.watch_dir):
            for filename in filenames:
                if filename.endswith(".csv") and not filename.startswith(".tmp_"):
                    files.append(os.path.join(root, filename))
        return sorted(files)

    def move_to_processed(self, filepath):
        dest = os.path.join(self.processed_dir, os.path.basename(filepath))
        shutil.move(filepath, dest)
        print(f"[MERGE] Moved {os.path.basename(filepath)} to processed.")

    def remove_file(self, filepath):
       os.remove(filepath)
       print(f"[MERGE] Removed {os.path.basename(filepath)} after processing.")

    def compute_throughput(self, batch_MB, duration):
        return batch_MB / duration if duration > 0 else 0

    def save_metrics_to_csv(self, throughput_MBps, late_rate, out_of_order_rate, duplicate_rate, batch_size, timestamp):
        metrics_file = os.path.join(self.metrics_dir, "merge_metrics.csv")
        metrics_data = {
            "timestamp": [datetime.utcfromtimestamp(timestamp).isoformat()],
            "throughput_MBps": [throughput_MBps],
            "late_rate": [late_rate],
            "out_of_order_rate": [out_of_order_rate],
            "duplicate_rate": [duplicate_rate],
            "batch_size": [batch_size],
            "uptime_seconds": [time.time() - self.start_time],
            "cpu_percent": [psutil.cpu_percent()],
            "memory_percent": [psutil.virtual_memory().percent]
        }
        df = pd.DataFrame(metrics_data)

        if not os.path.exists(metrics_file):
            df.to_csv(metrics_file, index=False)
        else:
            df.to_csv(metrics_file, mode='a', header=False, index=False)

    def merge_csv_files(self, files):
        dfs, total_bytes = [], 0
        for f in files:
            try:
                total_bytes += os.path.getsize(f)
                df = pd.read_csv(f)
                if not {'index', 'producer_timestamp', 'consumer_receive_timestamp'}.issubset(df.columns):
                    continue
                df['producer_timestamp'] = pd.to_numeric(df['producer_timestamp'], errors='coerce')
                df['index'] = pd.to_numeric(df['index'], errors='coerce')
                df['consumer_receive_timestamp'] = pd.to_numeric(df['consumer_receive_timestamp'], errors='coerce')
                df['source_file'] = os.path.basename(f)
                dfs.append(df)
            except Exception as e:
                print(f"[ERROR] Failed to process {f}: {e}")

        if not dfs:
            return None, None

        merged_df = pd.concat(dfs, ignore_index=True)

        now = time.time()
        duration = now - self.last_merge_time
        batch_MB = total_bytes / (1024 * 1024)
        throughput_MBps = self.compute_throughput(batch_MB, duration)

        merged_df['is_late'] = merged_df['producer_timestamp'] < self.previous_max_producer_timestamp
        late_rate = merged_df['is_late'].mean()

        idx_array = merged_df['index'].dropna().astype(int).to_numpy()
        out_of_order_flags = np.zeros(len(idx_array), dtype=bool)
        out_of_order_flags[1:] = idx_array[1:] < idx_array[:-1]
        merged_df['is_out_of_order'] = False
        merged_df.loc[merged_df['index'].dropna().index, 'is_out_of_order'] = out_of_order_flags
        out_of_order_rate = out_of_order_flags.mean()

        duplicate_flags = merged_df['index'].duplicated()
        merged_df['is_duplicate'] = duplicate_flags
        duplicate_rate = duplicate_flags.mean()

        merged_df['merge_timestamp'] = now
        merged_df['end_to_end_delay_ms'] = (merged_df['merge_timestamp'] - merged_df['producer_timestamp'])*1000

        fname = f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        merged_path = os.path.join(self.merged_dir, fname)
        merged_df.to_csv(merged_path, index=False)
        print(f"[MERGE] Saved: {merged_path} with {len(merged_df)} rows")

        throughput_gauge.set(throughput_MBps)
        late_rate_gauge.set(late_rate)
        out_of_order_gauge.set(out_of_order_rate)
        duplicate_gauge.set(duplicate_rate)
        last_merge_time_gauge.set(now)
        last_batch_size_gauge.set(len(merged_df))

        self.previous_max_producer_timestamp = merged_df['producer_timestamp'].max()
        self.save_metrics_to_csv(
            throughput_MBps=throughput_MBps,
            late_rate=late_rate,
            out_of_order_rate=out_of_order_rate,
            duplicate_rate=duplicate_rate,
            batch_size=len(merged_df),
            timestamp=now
        )

        return merged_path, now

    def run(self):
        while self.running:
            try:
                files = self.get_unprocessed_files()
                now = time.time()
                if files and (len(files) >= self.min_files or now - self.last_merge_time >= self.interval):
                    merged_path, merge_ts = self.merge_csv_files(files)
                    if merged_path:
                        for f in files:
                            self.remove_file(f)
                        self.last_merge_time = merge_ts
                else:
                    time.sleep(2)
            except KeyboardInterrupt:
                print("[MERGE] Stopping merger.")
                self.stop()
            except Exception as e:
                print(f"[ERROR] {e}")
                time.sleep(2)

    def stop(self):
        self.running = False
        print("[MERGE] Merger stopped cleanly.")

def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)

def graceful_shutdown(signal_num, frame):
    print("[MERGE] Received shutdown signal. Stopping gracefully...")
    merger.stop()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

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

