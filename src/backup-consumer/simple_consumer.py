import json
import time
import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import socket
import signal
import sys

class SimpleLatencyConsumer:
    def __init__(self, topics, bootstrap_servers, group_id, max_messages, poll_timeout,
                 fetch_max_bytes, fetch_min_bytes, fetch_max_wait_ms,
                 output_dir, enable_auto_commit, auto_offset_reset, socket_timeout_ms,
                 max_poll_interval_ms):
        
        self.topics = topics
        self.output_dir = output_dir
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout / 1000  # ms to seconds
        self.metrics = []
        self.msg_consumed = 0
        self.bytes_consumed = 0
        self.pod_name = socket.gethostname()
        self.start_time = time.time()

        self.consumer = Consumer({
            'bootstrap.servers': ",".join(bootstrap_servers),
            'group.id': group_id,
            'enable.auto.commit': str(enable_auto_commit).lower(),
            'auto.offset.reset': auto_offset_reset,
            'socket.timeout.ms': socket_timeout_ms,
            'fetch.max.bytes': fetch_max_bytes,
            'fetch.min.bytes': fetch_min_bytes,
            'fetch.wait.max.ms': fetch_max_wait_ms,
            'max.poll.interval.ms': max_poll_interval_ms,
            'heartbeat.interval.ms': 3000,
            'session.timeout.ms': 10000,
            'topic.metadata.refresh.interval.ms': 30000,
            'client.id': self.pod_name,
        })

        self.consumer.subscribe(topics)

    def consume(self):
        print("[INFO] Starting consumer loop...")
        while True:
            try:
                msgs = self.consumer.consume(num_messages=20, timeout=self.poll_timeout)

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

                    try:
                        index = index.decode()
                        producer_ts = float(producer_ts.decode())
                        receive_ts = time.time()
                        latency = receive_ts - producer_ts
                        target_rate_val = float(target_rate.decode()) if target_rate else None
                    except Exception as e:
                        print(f"[WARN] Header parsing error: {e}")
                        continue

                    # Only measurement path before I/O
                    self.metrics.append({
                        "index": index,
                        "producer_timestamp": producer_ts,
                        "consumer_receive_timestamp": receive_ts,
                        "end_to_end_latency_seconds": latency,
                        "size_bytes": size_bytes.decode() if size_bytes else None,
                        "target_rate": target_rate_val
                    })

                    self.bytes_consumed += len(msg.value()) if msg.value() else 0
                    self.msg_consumed += 1

                    if self.msg_consumed % 100 == 0:
                        try:
                            self.consumer.commit(asynchronous=True)
                        except Exception as e:
                            print(f"[WARN] Commit failed: {e}")

                    if len(self.metrics) >= self.max_messages:
                        self._save_batch(self.metrics.copy())
                        self.metrics.clear()

            except Exception as e:
                print(f"[ERROR] Consume loop failed: {e}")
                time.sleep(1)

    def _save_batch(self, batch):
        df = pd.DataFrame(batch)
        if df.empty:
            return
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
        filename = f"batch_{self.pod_name}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        try:
            df.to_csv(filepath, index=False)
            print(f"[INFO] Saved batch to {filepath}")
        except Exception as e:
            print(f"[ERROR] Saving batch failed: {e}")

# Graceful shutdown handler
def shutdown_handler(signal_num, frame):
    print("[INFO] Shutdown requested.")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('--topics', required=True)
    parser.add_argument('--uris', required=True)
    parser.add_argument('--groupId', default="consumer-group")
    parser.add_argument('--maxMsg', type=int, default=100)
    parser.add_argument('--pollTimeout', type=int, default=300)
    parser.add_argument('--maxPollIntervalMs', type=int, default=300000)
    parser.add_argument('--socketTimeoutMs', type=int, default=60000)
    parser.add_argument('--fetchMaxBytes', type=int, default=10485760)
    parser.add_argument('--fetchMinBytes', type=int, default=1024)
    parser.add_argument('--fetchMaxWaitMs', type=int, default=50)
    parser.add_argument('--consumerOutputDir', required=True)
    parser.add_argument('--enableAutoCommit', default="false")
    parser.add_argument('--autoOffsetReset', default="earliest")
    args = parser.parse_args()

    enable_auto_commit = args.enableAutoCommit.lower() == "true"

    consumer = SimpleLatencyConsumer(
        topics=args.topics.split(','),
        bootstrap_servers=args.uris.split(','),
        group_id=args.groupId,
        max_messages=args.maxMsg,
        poll_timeout=args.pollTimeout,
        fetch_max_bytes=args.fetchMaxBytes,
        fetch_min_bytes=args.fetchMinBytes,
        fetch_max_wait_ms=args.fetchMaxWaitMs,
        output_dir=args.consumerOutputDir,
        enable_auto_commit=enable_auto_commit,
        auto_offset_reset=args.autoOffsetReset,
        socket_timeout_ms=args.socketTimeoutMs,
        max_poll_interval_ms=args.maxPollIntervalMs
    )

    time.sleep(2)
    consumer.consume()

