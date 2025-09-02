import json
import time
import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, TopicPartition
import psutil
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge, Histogram, Summary
import socket
import threading
import signal
import sys
import queue
import random
import csv  # used by optional CSV saver

# Global consumer instance used for graceful shutdown
consumer_instance = None

# Flask app for exposing Prometheus metrics
app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

# ===== Prometheus metrics (with labels) =====
COMMON_LABELS = ['pod', 'group', 'client_id']

# End-to-end latency (per-pod, Summary in ms)
e2e_latency_per_pod = Summary(
    'e2e_latency_per_pod_ms',
    'End-to-end latency per pod in milliseconds (producer->consumer + application)',
    COMMON_LABELS
)

# End-to-end latency (aggregatable across pods, Histogram in seconds)
e2e_latency_across_pods = Histogram(
    'e2e_latency_across_pods_seconds',
    'End-to-end latency in seconds (producer->consumer + application), aggregatable across pods',
    COMMON_LABELS,
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2]
)

# Producer -> Consumer (network/broker path only)
net_latency_histogram = Histogram(
    'consumer_net_latency_seconds',
    'Producer→consumer latency (network/broker path only)',
    COMMON_LABELS,
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2]
)

# Application-only delay
app_delay_histogram = Histogram(
    'consumer_application_delay_seconds',
    'Application processing delay only (simulated or realistic)',
    COMMON_LABELS,
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2]
)

msg_consumed_counter = Counter(
    'consumer_messages_consumed_total',
    'Total messages consumed',
    COMMON_LABELS
)
msg_rate_gauge = Gauge(
    'consumer_message_rate',
    'Message consumption rate (msg/sec)',
    COMMON_LABELS
)
mb_rate_gauge = Gauge(
    'consumer_mb_rate',
    'Message throughput rate (MB/sec)',
    COMMON_LABELS
)
cpu_gauge = Gauge(
    'consumer_cpu_percent',
    'CPU percent usage',
    COMMON_LABELS
)
mem_gauge = Gauge(
    'consumer_memory_percent',
    'Memory percent usage',
    COMMON_LABELS
)
uptime_gauge = Gauge(
    'consumer_uptime_seconds',
    'Consumer uptime in seconds',
    COMMON_LABELS
)

out_of_order_rate_gauge = Gauge(
    'consumer_out_of_order_rate',
    'Fraction of out-of-order messages',
    COMMON_LABELS
)
duplicate_rate_gauge = Gauge(
    'consumer_duplicate_rate',
    'Fraction of duplicate messages',
    COMMON_LABELS
)
target_rate_gauge = Gauge(
    'consumer_target_rate',
    'Target message rate (msgs/sec)',
    COMMON_LABELS
)
consumer_lag_gauge = Gauge(
    'consumer_lag',
    'Consumer lag (latest offset - committed offset)',
    COMMON_LABELS + ['topic', 'partition']
)

# Config/state “freeze” gauges so each run is self-describing
fetch_min_bytes_gauge  = Gauge('consumer_cfg_fetch_min_bytes', 'fetch.min.bytes', COMMON_LABELS)
fetch_wait_ms_gauge    = Gauge('consumer_cfg_fetch_wait_ms', 'fetch.wait.max.ms', COMMON_LABELS)
poll_timeout_ms_gauge  = Gauge('consumer_cfg_poll_timeout_ms', 'poll timeout ms', COMMON_LABELS)
assigned_partitions_gauge = Gauge(
    'consumer_assigned_partitions',
    'Number of partitions assigned to this consumer',
    COMMON_LABELS
)


class MetricConsumer:
    def __init__(self, topics, servers, group_id, max_messages, session_timeout_ms, poll_timeout_ms, fetch_max_bytes,
                 fetch_min_bytes, fetch_max_wait_ms, consumer_output_dir, enable_auto_commit,
                 auto_offset_reset, socket_timeout_ms, max_poll_interval_ms, lag_query_timeout, lag_query_interval,
                 heartbeat_interval_ms, partition_assignment_strategy, app_delay_min_s, app_delay_max_s, app_delay_mode):

        self.topics = topics
        self.poll_timeout = poll_timeout_ms / 1000.0
        self.consumer_output_dir = consumer_output_dir
        self.pod_name = socket.gethostname()
        self.group_id = group_id
        self.client_id = self.pod_name
        self.metric_labels = {'pod': self.pod_name, 'group': self.group_id, 'client_id': self.client_id}

        self.start_time = time.time()
        self.msg_consumed = 0
        self.bytes_consumed = 0
        self.last_log_time = time.time()
        self.last_lag_update = 0
        self.metrics_list = []
        self.max_messages = max_messages
        self.lag_query_timeout = float(lag_query_timeout)
        self.lag_query_interval = int(lag_query_interval)
        
        self.app_delay_mode = app_delay_mode.lower()
        if self.app_delay_mode not in ("simulated", "realistic"):
            raise ValueError("Invalid appDelayMode, must be 'simulated' or 'realistic'")

        # Simulated application delay range (seconds)
        self.app_delay_min_s = float(app_delay_min_s)
        self.app_delay_max_s = float(app_delay_max_s)
        if self.app_delay_min_s < 0 or self.app_delay_max_s < 0 or self.app_delay_min_s > self.app_delay_max_s:
            raise ValueError("Invalid app delay range; ensure 0 <= min <= max")

        # Async saver so file I/O never blocks the consume loop
        self._sentinel = object()
        self.save_queue = queue.Queue(maxsize=4096)
        self.save_thread = threading.Thread(target=self.save_worker, daemon=True)
        self.save_thread.start()

        self.consumer = Consumer({
            'bootstrap.servers': ",".join(servers),
            'group.id': group_id,
            'enable.auto.commit': 'true' if enable_auto_commit else 'false',
            'auto.offset.reset': auto_offset_reset,
            'socket.timeout.ms': int(socket_timeout_ms),
            'fetch.max.bytes': int(fetch_max_bytes),
            'fetch.min.bytes': int(fetch_min_bytes),
            'fetch.wait.max.ms': int(fetch_max_wait_ms),
            'max.poll.interval.ms': int(max_poll_interval_ms),
            'heartbeat.interval.ms': int(heartbeat_interval_ms),  # ~ 1/3 of session.timeout.ms
            'session.timeout.ms': int(session_timeout_ms),
            'client.id': self.client_id,
            'socket.send.buffer.bytes': 524288,
            'socket.receive.buffer.bytes': 524288,
            'partition.assignment.strategy': partition_assignment_strategy,
        })

        self.consumer.subscribe(topics)

        # Ensure output dir exists
        os.makedirs(self.consumer_output_dir, exist_ok=True)

        # Freeze config gauges (set once at init)
        fetch_min_bytes_gauge.labels(**self.metric_labels).set(int(fetch_min_bytes))
        fetch_wait_ms_gauge.labels(**self.metric_labels).set(int(fetch_max_wait_ms))
        poll_timeout_ms_gauge.labels(**self.metric_labels).set(int(poll_timeout_ms))

    def update_consumer_lag(self):
        try:
            assignments = self.consumer.assignment()
            # track assigned partitions
            assigned_partitions_gauge.labels(**self.metric_labels).set(len(assignments or []))

            if not assignments:
                return

            committed = self.consumer.committed(assignments, timeout=self.lag_query_timeout)
            for tp, committed_tp in zip(assignments, committed):
                low, high = self.consumer.get_watermark_offsets(tp, timeout=self.lag_query_timeout)
                if committed_tp.offset != KafkaError._NO_OFFSET:
                    lag = max(0, high - committed_tp.offset)
                    consumer_lag_gauge.labels(
                        **self.metric_labels,
                        topic=tp.topic,
                        partition=str(tp.partition)
                    ).set(lag)
        except Exception as e:
            print(f"[WARN] Failed to update consumer lag: {e}")

    def consume(self):
        while True:
            try:
                msgs = self.consumer.consume(num_messages=1000, timeout=self.poll_timeout)
                now = time.time()
                elapsed = now - self.start_time

                # Update system/throughput metrics every 5s
                if now - self.last_log_time >= 5:
                    cpu_gauge.labels(**self.metric_labels).set(psutil.cpu_percent(interval=None))
                    mem_gauge.labels(**self.metric_labels).set(psutil.virtual_memory().percent)
                    uptime_gauge.labels(**self.metric_labels).set(elapsed)

                    if elapsed > 0:
                        msg_rate_gauge.labels(**self.metric_labels).set(self.msg_consumed / elapsed)
                        mb_rate_gauge.labels(**self.metric_labels).set((self.bytes_consumed / (1024 * 1024)) / elapsed)
                    self.last_log_time = now

                # Update lag less frequently (configurable)
                if now - self.last_lag_update >= self.lag_query_interval:
                    self.update_consumer_lag()
                    self.last_lag_update = now

                for msg in msgs:
                    if msg is None or msg.error():
                        continue

                    headers = dict(msg.headers() or [])
                    producer_pod_hdr = headers.get('producer_pod')
                    index = headers.get('index')
                    producer_ts = headers.get('producer_timestamp')
                    size_bytes = headers.get('size_bytes')
                    target_rate = headers.get('target_rate')
                    producer_pod = headers.get('producer_pod_name')
                    producer_pod = (producer_pod.decode() if isinstance(producer_pod, (bytes, bytearray)) else producer_pod) if producer_pod else None
                    consumer_pod = self.pod_name

                    if index is None or producer_ts is None:
                        continue

                    # decode headers
                    try:
                        index = int(index.decode())
                    except Exception:
                        index = (index.decode() if isinstance(index, (bytes, bytearray)) else str(index))

                    producer_ts = float(producer_ts.decode()) if isinstance(producer_ts, (bytes, bytearray)) else float(producer_ts)
                    receive_ts = time.time()
                    network_latency_sec = receive_ts - producer_ts

                    # ---- Simulate application processing time (no sleep; just add to timestamps) ----
                    app_delay = random.uniform(self.app_delay_min_s, self.app_delay_max_s)  # seconds
                   
                    if self.app_delay_mode == "realistic":
                        time.sleep(app_delay)   # actually wait
                    
                    application_ts = receive_ts + app_delay
                    end_to_end_latency_sec = network_latency_sec + app_delay

                    # Metrics (component-wise + e2e)
                    net_latency_histogram.labels(**self.metric_labels).observe(network_latency_sec)
                    app_delay_histogram.labels(**self.metric_labels).observe(app_delay)

                    # Summary in ms (per pod)
                    e2e_latency_per_pod.labels(**self.metric_labels).observe(end_to_end_latency_sec * 1000.0)
                    # Histogram in seconds (aggregatable)
                    e2e_latency_across_pods.labels(**self.metric_labels).observe(end_to_end_latency_sec)

                    if target_rate:
                        try:
                            target_rate_val = float(target_rate.decode()) if isinstance(target_rate, (bytes, bytearray)) else float(target_rate)
                            target_rate_gauge.labels(**self.metric_labels).set(target_rate_val)
                        except Exception:
                            pass
                    producer_pod_val = None
                    if producer_pod_hdr is not None:
                        producer_pod_val = (producer_pod_hdr.decode()
                                        if isinstance(producer_pod_hdr, (bytes, bytearray))
                                        else str(producer_pod_hdr))

                    # Append lightweight record for async save
                    self.metrics_list.append({
                        "index": index,
                        "producer_timestamp": producer_ts,
                        "consumer_receive_timestamp": receive_ts,
                        "application_timestamp": application_ts,              
                        "application_latency_seconds": app_delay,             
                        "end_to_end_latency_seconds": end_to_end_latency_sec, 
                        "size_bytes": (int(size_bytes.decode()) if isinstance(size_bytes, (bytes, bytearray)) else None) if size_bytes else None,
                        "target_rate": (float(target_rate.decode()) if isinstance(target_rate, (bytes, bytearray)) else None) if target_rate else None,
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "producer_pod": producer_pod,
                        "consumer_pod": consumer_pod,
                        "producer_pod": producer_pod_val,
                        "consumer_pod": self.pod_name,
                    })

                    # Counters/bytes
                    self.bytes_consumed += len(msg.value()) if msg.value() else 0
                    self.msg_consumed += 1
                    msg_consumed_counter.labels(**self.metric_labels).inc()

                    # Periodic commit (asynchronous)
                    if self.msg_consumed % 100 == 0:
                        try:
                            self.consumer.commit(asynchronous=True)
                        except Exception as e:
                            print(f"[WARN] Commit failed: {e}")

                    # Hand off to saver without blocking
                    if len(self.metrics_list) >= self.max_messages:
                        batch = self.metrics_list
                        self.metrics_list = []
                        try:
                            self.save_queue.put(batch, timeout=0.2)  # short block to avoid drops
                        except queue.Full:
                            self.save_queue.put(batch, timeout=1.0)  # last resort

            except Exception as e:
                print(f"[ERROR] Consume loop error: {e}")
                time.sleep(0.5)

    # Background thread that asynchronously saves consumed message batches to disk to avoid blocking the main consume loop
    def save_worker(self):
        while True:
            batch = self.save_queue.get()
            if batch is self._sentinel:
                break
            try:
                self._save_parquet_to_disk(batch)
            except Exception as e:
                print(f"[ERROR] Save worker failed: {e}")

    def _save_parquet_to_disk(self, batch):
        df = pd.DataFrame(batch)
        if df.empty:
            return
        # Ensure pod columns exist and are strings
        if 'producer_pod' not in df.columns:
            df['producer_pod'] = None
        if 'consumer_pod' not in df.columns:
            df['consumer_pod'] = self.pod_name
        
        # force string dtype so pyarrow doesn’t drop all-null columns weirdly
        df['producer_pod'] = df['producer_pod'].astype('string')
        df['consumer_pod'] = df['consumer_pod'].astype('string')
        
        now = datetime.now()
        filename = f"batch_{self.pod_name}_{now.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        tmp_path = os.path.join(self.consumer_output_dir, f".tmp_{filename}")
        final_path = os.path.join(self.consumer_output_dir, filename)
        df.to_parquet(tmp_path, index=False)  # fast + compressed
        os.replace(tmp_path, final_path)
    
    
    def _save_batch_to_disk(self, batch):
        """Optional CSV saver (not used by default)."""
        df = pd.DataFrame(batch)
        if df.empty:
            return

        now = datetime.now()
        filename = f"batch_{self.pod_name}_{now.strftime('%Y%m%d_%H%M%S_%f')}.csv"
        tmp_path = os.path.join(self.consumer_output_dir, f".tmp_{filename}")
        final_path = os.path.join(self.consumer_output_dir, filename)

        elapsed = time.time() - self.start_time
        df["msg_rate_per_sec"] = (self.msg_consumed / elapsed) if elapsed > 0 else 0.0
        df["mb_rate_per_sec"] = ((self.bytes_consumed / (1024 * 1024)) / elapsed) if elapsed > 0 else 0.0

        # Out-of-order / duplicate flags (based on index column)
        if 'index' in df.columns:
            idx_series = pd.to_numeric(df['index'], errors='coerce')
            idx_array = idx_series.dropna().astype(int).to_numpy()
            out_of_order_flags = np.zeros(len(idx_array), dtype=bool)
            if len(idx_array) > 1:
                out_of_order_flags[1:] = idx_array[1:] < idx_array[:-1]
            df['is_out_of_order'] = False
            df.loc[idx_series.dropna().index, 'is_out_of_order'] = out_of_order_flags
            df['is_duplicate'] = idx_series.duplicated()

            # Update gauges with *current batch* rates (per consumer)
            out_of_order_rate_gauge.labels(**self.metric_labels).set(float(df['is_out_of_order'].mean()))
            df_dup_mean = float(df['is_duplicate'].mean())
            duplicate_rate_gauge.labels(**self.metric_labels).set(df_dup_mean)

        # Atomic write
        try:
            df.to_csv(
                tmp_path,
                index=False,
                line_terminator='\n',
                quoting=csv.QUOTE_MINIMAL
            )
            os.replace(tmp_path, final_path)
            print(f"[INFO] Saved: {final_path} ({len(df)} records)")
        except Exception as e:
            print(f"[ERROR] Failed to save batch: {e}")

    def shutdown(self):
        # Signal saver to stop and flush
        try:
            self.save_queue.put(self._sentinel)
            self.save_thread.join(timeout=5)
        except Exception:
            pass
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"[WARN] Commit on shutdown failed: {e}")
        try:
            self.consumer.close()
        except Exception as e:
            print(f"[WARN] Consumer close failed: {e}")


def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)


def graceful_shutdown(signal_num, frame):
    print("[INFO] Shutting down gracefully...")
    if consumer_instance:
        consumer_instance.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGQUIT, graceful_shutdown)

    parser = argparse.ArgumentParser()
    parser.add_argument('--topics', required=True)
    parser.add_argument('--uris', required=True)
    parser.add_argument('--groupId', default="consumer-group")
    parser.add_argument('--maxMsg', type=int, default=100)
    parser.add_argument('--pollTimeout', type=int, default=300)
    parser.add_argument('--maxPollIntervalMs', type=int, default=120000)
    parser.add_argument('--heartbeatIntervalMs', type=int, default=3000)
    parser.add_argument('--sessionTimeoutMs', type=int, default=10000)
    parser.add_argument('--socketTimeoutMs', type=int, default=60000)
    parser.add_argument('--fetchMaxBytes', type=int, default=10485760)
    parser.add_argument('--fetchMinBytes', type=int, default=1024)
    parser.add_argument('--fetchMaxWaitMs', type=int, default=500)
    parser.add_argument('--consumerOutputDir', required=True)
    parser.add_argument('--enableAutoCommit', default="false")
    parser.add_argument('--autoOffsetReset', default="earliest")
    parser.add_argument('--lagQueryTimeout', type=float, default=5.0)
    parser.add_argument('--lagQueryInterval', type=int, default=30)  # seconds
    parser.add_argument('--partitionAssignStrategy', type=str, default="cooperative-sticky")

    # application processing delay range (ms) and mode 
    parser.add_argument('--appDelayMinMs', type=float, default=1.0, help="Minimum simulated application delay in milliseconds (default: 1)")
    parser.add_argument('--appDelayMaxMs', type=float, default=100.0, help="Maximum simulated application delay in milliseconds (default: 100)")
    parser.add_argument('--appDelayMode', type=str, default="simulated", help="Delay mode: 'simulated' (no sleep, just record) or 'realistic' (sleep before marking processed)")

    args = parser.parse_args()

    enable_auto_commit = args.enableAutoCommit.lower() == 'true'

    # Convert ms -> seconds for internal use
    app_delay_min_s = args.appDelayMinMs / 1000.0
    app_delay_max_s = args.appDelayMaxMs / 1000.0

    consumer_instance = MetricConsumer(
        topics=args.topics.split(','),
        servers=[s.strip() for s in args.uris.split(',') if s.strip()],
        group_id=args.groupId,
        max_messages=args.maxMsg,
        session_timeout_ms=args.sessionTimeoutMs,
        poll_timeout_ms=args.pollTimeout,
        fetch_max_bytes=args.fetchMaxBytes,
        fetch_min_bytes=args.fetchMinBytes,
        fetch_max_wait_ms=args.fetchMaxWaitMs,
        consumer_output_dir=args.consumerOutputDir,
        enable_auto_commit=enable_auto_commit,
        auto_offset_reset=args.autoOffsetReset,
        socket_timeout_ms=args.socketTimeoutMs,
        max_poll_interval_ms=args.maxPollIntervalMs,
        lag_query_timeout=args.lagQueryTimeout,
        lag_query_interval=args.lagQueryInterval,
        heartbeat_interval_ms=args.heartbeatIntervalMs,
        partition_assignment_strategy=args.partitionAssignStrategy,
        app_delay_min_s=app_delay_min_s,
        app_delay_max_s=app_delay_max_s,
        app_delay_mode=args.appDelayMode
    )

    threading.Thread(target=start_metrics_server, daemon=True).start()
    consumer_instance.consume()

