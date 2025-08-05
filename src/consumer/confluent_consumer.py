import json
import time
import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import helper
import psutil
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge, Histogram
import socket
import threading
import signal
import sys
import queue

# Global variable to be set in __main__ and used in graceful_shutdown
consumer_instance = None

app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

msg_consumed_counter = Counter('consumer_messages_consumed_total', 'Total messages consumed')
msg_rate_gauge = Gauge('consumer_message_rate', 'Message consumption rate (msg/sec)')
mb_rate_gauge = Gauge('consumer_mb_rate', 'Message throughput rate (MB/sec)')
cpu_gauge = Gauge('consumer_cpu_percent', 'CPU percent usage')
mem_gauge = Gauge('consumer_memory_percent', 'Memory percent usage')
uptime_gauge = Gauge('consumer_uptime_seconds', 'Consumer uptime in seconds')
consumer_latency_histogram = Histogram('consumer_latency_seconds', 'End-to-end latency (producer to consumer) in seconds', buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2])
out_of_order_rate_gauge = Gauge('consumer_out_of_order_rate', 'Fraction of out-of-order messages')
duplicate_rate_gauge = Gauge('consumer_duplicate_rate', 'Fraction of duplicate messages')
target_rate_gauge = Gauge('consumer_target_rate', 'Target message rate (msgs/sec)')  # <-- NEW

class MetricConsumer:
    def __init__(self, topics, servers, group_id, max_messages, poll_timeout, fetch_max_bytes,
                 fetch_min_bytes, fetch_max_wait_ms, consumer_output_dir, enable_auto_commit, auto_offset_reset, socket_timeout_ms,max_poll_interval_ms):
        self.topics = topics
        self.metrics_list = []
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout / 1000
        self.consumer_output_dir = consumer_output_dir
        self.pod_name = socket.gethostname()
        self.start_time = time.time()
        self.msg_consumed = 0
        self.bytes_consumed = 0
        self.last_log_time = time.time()
        self.save_queue = queue.Queue()
        #self.save_thread = threading.Thread(target=self.save_worker)
        #self.save_thread.start()

        #config = helper.Tools().read_config('consumer.properties')
        #jaas_config = config.get('sasl.jaas.config', '')
        #username = jaas_config.split('username=')[1].split(' ')[0].replace('"', '')
        #password = jaas_config.split('password=')[1].replace('"', '').replace(';', '')
       
        self.consumer = Consumer({
            'bootstrap.servers': "pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            'group.id': group_id,
            'enable.auto.commit': 'true' if enable_auto_commit else 'false',
            'auto.offset.reset': auto_offset_reset,
            'socket.timeout.ms': int(socket_timeout_ms),
            #'security.protocol': config.get('security.protocol', 'PLAINTEXT'),
            #'sasl.mechanism': config.get('sasl.mechanism', 'PLAIN'),
            #'sasl.username': username,
            #'sasl.password': password,
            'fetch.max.bytes':  int(fetch_max_bytes),
            'fetch.min.bytes':  int(fetch_min_bytes),
            'fetch.wait.max.ms':  int(fetch_max_wait_ms),
            'max.poll.interval.ms':  int(max_poll_interval_ms),
            'heartbeat.interval.ms': 3000,
            'session.timeout.ms': 10000,
            'topic.metadata.refresh.interval.ms': 30000,
            #'security.protocol': "SASL_PLAINTEXT",
            #'sasl.mechanism': "PLAIN",
            #'sasl.username': "user1",
            #'sasl.password': "5x4XjjbPod",
            'debug': "security,broker",
            'client.id': socket.gethostname()
        })

        self.consumer.subscribe(topics)

    def consume(self):
        last_metrics_update = time.time()
        while True:
            try:
                msgs = self.consumer.consume(num_messages=20, timeout=self.poll_timeout)
                now = time.time()

                if now - last_metrics_update >= 5:
                    elapsed = now - self.start_time
                    cpu_gauge.set(psutil.cpu_percent(interval=1))
                    mem_gauge.set(psutil.virtual_memory().percent)
                    uptime_gauge.set(elapsed)

                    msg_rate = self.msg_consumed / elapsed if elapsed > 0 else 0
                    mb_rate = (self.bytes_consumed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                    msg_rate_gauge.set(msg_rate)
                    mb_rate_gauge.set(mb_rate)

                    last_metrics_update = now

                if now - self.last_log_time > 10:
                    print(f"[HEALTH] Polling active. Messages consumed: {self.msg_consumed}")
                    self.last_log_time = now

                for msg in msgs:
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[ERROR] Poll error: {msg.error()}")
                        continue

                    headers = dict(msg.headers() or [])
                    index = headers.get('index')
                    producer_ts = headers.get('producer_timestamp')
                    size_bytes = headers.get('size_bytes')
                    target_rate = headers.get('target_rate')

                    if index is None or producer_ts is None:
                        continue
                    
                    index = index.decode()
                    producer_ts = float(producer_ts.decode())
                    
                    target_rate_value = float(target_rate.decode()) if target_rate else None  
                    if target_rate_value is not None:
                        target_rate_gauge.set(target_rate_value)
                    else:
                        print(f"[WARN] Missing target_rate in message with index {index}")

                    receive_ts = time.time()
                    latency = receive_ts - producer_ts
                    consumer_latency_histogram.observe(latency)

                    self.metrics_list.append({
                        "index": index,
                        "producer_timestamp": producer_ts,
                        "consumer_receive_timestamp": receive_ts,
                        "end_to_end_latency_seconds": latency,
                        "size_bytes": size_bytes,
                        "target_rate": target_rate_value
                    })

                    self.bytes_consumed += len(msg.value()) if msg.value() else 0
                    self.msg_consumed += 1
                    msg_consumed_counter.inc()
    
                    # Commit every 100 messages asynchronously
                    if self.msg_consumed % 100 == 0:
                        try:
                            self.consumer.commit(asynchronous=True)
                            self.consumer.poll(0)  # give librdkafka a chance to flush internal queue
                        except Exception as e:
                            print(f"[WARN] Async commit failed at {self.msg_consumed} msgs: {e}")

                    # allow time for background work and reduce CPU pressure
                    if self.msg_consumed % 200 == 0:
                        time.sleep(0.05)

                    start_commit = time.time()
                    commit_duration = time.time() - start_commit

                    if commit_duration > 0.5:
                        print(f"[WARN] Commit took {commit_duration:.3f} seconds!")

                    if len(self.metrics_list) >= self.max_messages:
                        self._save_batch_to_disk(self.metrics_list.copy())
                        self.metrics_list.clear()

            except Exception as e:
                print(f"[ERROR] Consume loop error: {e}")
                time.sleep(1)

    def save_worker(self):
        while True:
            try:
                batch = self.save_queue.get()
                if not batch:
                    print("[INFO] Save thread exiting cleanly.")
                    break
                self._save_batch_to_disk(batch)
            except Exception as e:
                print(f"[ERROR] Save worker failed: {e}")

    def _save_batch_to_disk(self, batch):
        df = pd.DataFrame(batch)
        if df.empty:
            return
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
        filename = f"batch_{self.pod_name}_{timestamp}.csv"
        tmp_path = os.path.join(self.consumer_output_dir, f".tmp_{filename}")
        final_path = os.path.join(self.consumer_output_dir, filename)
        elapsed = time.time() - self.start_time

        msg_rate = self.msg_consumed / elapsed if elapsed > 0 else 0
        mb_rate = (self.bytes_consumed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
        df["msg_rate_per_sec"] = msg_rate
        df["mb_rate_per_sec"] = mb_rate

        df['index'] = pd.to_numeric(df['index'], errors='coerce')
        df['target_rate'] = pd.to_numeric(df['target_rate'], errors='coerce')
        idx_array = df['index'].dropna().astype(int).to_numpy()
        out_of_order_flags = np.zeros(len(idx_array), dtype=bool)
        out_of_order_flags[1:] = idx_array[1:] < idx_array[:-1]
        df['is_out_of_order'] = False
        df.loc[df['index'].dropna().index, 'is_out_of_order'] = out_of_order_flags
        df['is_duplicate'] = df['index'].duplicated()

        out_of_order_rate = df['is_out_of_order'].mean()
        duplicate_rate = df['is_duplicate'].mean()
        out_of_order_rate_gauge.set(out_of_order_rate)
        duplicate_rate_gauge.set(duplicate_rate)

        try:
            df.to_csv(tmp_path, index=False)
            os.rename(tmp_path, final_path)
            print(f"[INFO] Async saved: {final_path} with {len(df)} records")
        except Exception as e:
            print(f"[ERROR] Failed to save batch: {e}")

def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)

def graceful_shutdown(signal_num, frame):
    print("[INFO] Shutting down gracefully...")
    if consumer_instance:
        consumer_instance.save_queue.put([])  # Send empty to signal termination
        consumer_instance.save_thread.join()
        
        #  Final offset commit
        print("[INFO] Final commit before shutdown...")
        try:
            consumer_instance.consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"[WARN] Final commit failed: {e}")

        #  Properly close the consumer
        try:
            consumer_instance.consumer.close()
            print("[INFO] Consumer closed cleanly.")
        except Exception as e:
            print(f"[WARN] Error while closing consumer: {e}")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    parser = argparse.ArgumentParser()
    parser.add_argument('--topics', type=str, required=True)
    parser.add_argument('--uris', type=str, required=True)
    parser.add_argument('--groupId', type=str, default="consumer-group")
    parser.add_argument('--maxMsg', type=int, default=100)
    parser.add_argument('--maxPoolRecords', type=int, default=500)
    parser.add_argument('--pollTimeout', type=int, default=300)
    parser.add_argument('--maxPollIntervalMs', type=int, default=300000)
    #parser.add_argument('--queuedMinMessages', type=int, default=1000)
    parser.add_argument('--socketTimeoutMs', type=int, default=60000)
    parser.add_argument('--sessionTimeoutMs', type=int, default=10000)
    parser.add_argument('--fetchMaxBytes', type=int, default=10485760)
    parser.add_argument('--fetchMinBytes', type=int, default=1024)
    parser.add_argument('--fetchMaxWaitMs', type=int, default=500)
    parser.add_argument('--consumerOutputDir', type=str, required=True)
    parser.add_argument('--enableAutoCommit', type=str, default="false")
    parser.add_argument('--autoCommitIntervalMs', type=int, default=10000)
    parser.add_argument('--autoOffsetReset', type=str, default="earliest")
    args = parser.parse_args()
    enable_auto_commit = args.enableAutoCommit.lower() == 'true'

    consumer_instance = MetricConsumer(
        topics=args.topics.split(','),
        servers=[s.strip() for s in args.uris.split(',') if s.strip()],
        group_id=args.groupId,
        max_messages=args.maxMsg,
        poll_timeout=args.pollTimeout,
        fetch_max_bytes=args.fetchMaxBytes,
        fetch_min_bytes=args.fetchMinBytes,
        fetch_max_wait_ms=args.fetchMaxWaitMs,
        consumer_output_dir=args.consumerOutputDir,
        enable_auto_commit= enable_auto_commit, # Pass boolean
        auto_offset_reset=args.autoOffsetReset,
        socket_timeout_ms=args.socketTimeoutMs,
        #queued_min_messages=args.queuedMinMessages,
        max_poll_interval_ms=args.maxPollIntervalMs
    )
    print(args)
    #threading.Thread(target=start_metrics_server, daemon=True).start()
    time.sleep(5)  #delay to avoid rejoin race after pod restart
    consumer_instance.consume()
