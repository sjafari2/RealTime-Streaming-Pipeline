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

app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

# Explicitly defined Prometheus metrics
msg_consumed_counter = Counter('consumer_messages_consumed_total', 'Total messages consumed')
msg_rate_gauge = Gauge('consumer_message_rate', 'Message consumption rate (msg/sec)')
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
        while True:
            try:
                msgs = self.consumer.consume(num_messages=20, timeout=self.poll_timeout)
                now = time.time()

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
                    index = headers.get(b'index', b'').decode() if headers.get(b'index') else ''
                    producer_ts = headers.get(b'producer_timestamp', b'').decode() if headers.get(b'producer_timestamp') else ''
                    receive_ts = str(time.time())

                    if index == '' or producer_ts == '':
                        continue

                    self.metrics_list.append({
                        "index": index,
                        "producer_timestamp": producer_ts,
                        "consumer_receive_timestamp": receive_ts
                    })

                    self.msg_consumed += 1
                    msg_consumed_counter.inc()

                    elapsed = now - self.start_time
                    msg_rate = self.msg_consumed / elapsed if elapsed > 0 else 0

                    msg_rate_gauge.set(msg_rate)
                    cpu_gauge.set(psutil.cpu_percent(interval=None))
                    mem_gauge.set(psutil.virtual_memory().percent)
                    uptime_gauge.set(elapsed)

                    self.consumer.commit(message=msg, asynchronous=False)

                    if len(self.metrics_list) >= self.max_messages:
                        self.save_batch()
                        self.metrics_list.clear()

            except Exception as e:
                print(f"[ERROR] Consume loop error: {e}")

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--topics', type=str, required=True)
    parser.add_argument('--uris', type=str, required=True)
    parser.add_argument('--groupId', type=str, default="consumer-group")
    parser.add_argument('--maxMsg', type=int, default=100)
    parser.add_argument('--pollTimeout', type=int, default=300)
    parser.add_argument('--fetchMaxBytes', type=int, default=10485760)
    parser.add_argument('--fetchMinBytes', type=int, default=1024)
    parser.add_argument('--fetchMaxWaitMs', type=int, default=500)
    parser.add_argument('--consumerOutputDir', type=str, required=True)
    parser.add_argument('--enableAutoCommit', type=str, default="false")
    parser.add_argument('--autoOffsetReset', type=str, default="earliest")
    args = parser.parse_args()

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
        enable_auto_commit=args.enableAutoCommit.lower() == "true",
        auto_offset_reset=args.autoOffsetReset
    )

    threading.Thread(target=start_metrics_server, daemon=True).start()
    consumer_instance.consume()
