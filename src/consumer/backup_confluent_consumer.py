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
from prometheus_client import Counter, Gauge
import socket
import threading
import signal
import sys

app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

msg_consumed_counter = Counter('consumer_messages_consumed_total', 'Total messages consumed')
msg_rate_gauge = Gauge('consumer_message_rate', 'Message consumption rate (msg/sec)')
mb_rate_gauge = Gauge('consumer_mb_rate', 'Real throughput (MB/sec)')
cpu_gauge = Gauge('consumer_cpu_percent', 'CPU percent usage')
mem_gauge = Gauge('consumer_memory_percent', 'Memory percent usage')
uptime_gauge = Gauge('consumer_uptime_seconds', 'Consumer uptime in seconds')

class MetricConsumer:
    def __init__(self, topics, servers, group_id, max_messages, poll_timeout, fetch_max_bytes,
                 fetch_min_bytes, fetch_max_wait_ms, consumer_output_dir, enable_auto_commit, auto_offset_reset):
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

        config = helper.Tools().read_config('consumer.properties')
        jaas_config = config.get('sasl.jaas.config', '')
        username = jaas_config.split('username=')[1].split(' ')[0].replace('"', '')
        password = jaas_config.split('password=')[1].replace('"', '').replace(';', '')

        self.consumer = Consumer({
            'bootstrap.servers': ','.join(servers),
            'group.id': group_id,
            'enable.auto.commit': enable_auto_commit,
            'auto.offset.reset': auto_offset_reset,
            'security.protocol': config.get('security.protocol', 'PLAINTEXT'),
            'sasl.mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl.username': username,
            'sasl.password': password,
            'fetch.max.bytes': fetch_max_bytes,
            'fetch.min.bytes': fetch_min_bytes,
            'fetch.wait.max.ms': fetch_max_wait_ms
        })

        self.consumer.subscribe(topics)

    def consume(self):
        last_metrics_update = time.time()
        while True:
            try:
                msgs = self.consumer.consume(num_messages=20, timeout=self.poll_timeout)
                now = time.time()

                if now - last_metrics_update >= 5:
                    cpu_gauge.set(psutil.cpu_percent(interval=1))
                    mem_gauge.set(psutil.virtual_memory().percent)
                    uptime_gauge.set(now - self.start_time)
                    elapsed = now - self.start_time
                    msg_rate = self.msg_consumed / elapsed if elapsed > 0 else 0
                    mb_rate = (self.bytes_consumed / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                    msg_rate_gauge.set(msg_rate)
                    mb_rate_gauge.set(mb_rate)
                    print(f"[METRIC] Elapsed: {elapsed:.2f}s | Consumed: {self.msg_consumed} msgs | Rate: {msg_rate:.2f} msg/s | Real Throughput: {mb_rate:.4f} MB/s")
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
                    if index is None or producer_ts is None:
                        continue
                    index = index.decode()
                    producer_ts = producer_ts.decode()

                    receive_ts = str(time.time())

                    self.metrics_list.append({
                        "index": index,
                        "producer_timestamp": producer_ts,
                        "consumer_receive_timestamp": receive_ts
                    })

                    self.msg_consumed += 1
                    self.bytes_consumed += len(msg.value()) if msg.value() else 0
                    msg_consumed_counter.inc()

                    self.consumer.commit(message=msg, asynchronous=False)

                    if len(self.metrics_list) >= self.max_messages:
                        self.save_batch()
                        self.metrics_list.clear()

            except Exception as e:
                print(f"[ERROR] Consume loop error: {e}")
                time.sleep(1)

    def save_batch(self):
        df = pd.DataFrame(self.metrics_list)
        if df.empty:
            return

        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
        filename = f"batch_{self.pod_name}_{timestamp}.csv"
        tmp_path = os.path.join(self.consumer_output_dir, f".tmp_{filename}")
        final_path = os.path.join(self.consumer_output_dir, filename)

        try:
            df.to_csv(tmp_path, index=False)
            os.rename(tmp_path, final_path)
            print(f"[INFO] Saved batch: {final_path} with {len(df)} records")
        except Exception as e:
            print(f"[ERROR] Failed to save batch: {e}")

def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)

def graceful_shutdown(signal_num, frame):
    print("[INFO] Shutting down gracefully...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    parser = argparse.ArgumentParser()
    parser.add_argument('--topics', nargs='+', required=True)
    parser.add_argument('--servers', nargs='+', required=True)
    parser.add_argument('--group_id', required=True)
    parser.add_argument('--max_messages', type=int, default=500)
    parser.add_argument('--poll_timeout', type=int, default=3000)
    parser.add_argument('--fetch_max_bytes', type=int, default=10485760)
    parser.add_argument('--fetch_min_bytes', type=int, default=1024)
    parser.add_argument('--fetch_max_wait_ms', type=int, default=100)
    parser.add_argument('--consumer_output_dir', required=True)
    parser.add_argument('--enable_auto_commit', type=bool, default=True)
    parser.add_argument('--auto_offset_reset', default='earliest')
    args = parser.parse_args()

    threading.Thread(target=start_metrics_server, daemon=True).start()

    consumer = MetricConsumer(
        topics=args.topics,
        servers=args.servers,
        group_id=args.group_id,
        max_messages=args.max_messages,
        poll_timeout=args.poll_timeout,
        fetch_max_bytes=args.fetch_max_bytes,
        fetch_min_bytes=args.fetch_min_bytes,
        fetch_max_wait_ms=args.fetch_max_wait_ms,
        consumer_output_dir=args.consumer_output_dir,
        enable_auto_commit=args.enable_auto_commit,
        auto_offset_reset=args.auto_offset_reset
    )
    consumer.consume()

