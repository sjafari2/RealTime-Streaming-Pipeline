import json
import time
import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import helper
import uuid


class ConfigLoader:
    @staticmethod
    def read_properties(filepath):
        config = {}
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
        return config


class MetricConsumer:
    def __init__(self, topics, servers, group_id, output_path, auto_commit, offset_reset,
                 auth_config, max_messages, poll_timeout, max_records,
                 fetch_max_bytes, fetch_min_bytes, fetch_max_wait_ms):
        self.topics = topics
        self.output_path = output_path
        self.metrics = []
        self.start_time = time.time()
        self.total_bytes = 0
        self.message_count = 0
        self.max_messages = max_messages
        self.poll_timeout = poll_timeout / 1000  # Convert ms to seconds
        self.max_records = max_records

        config = helper.Tools().read_config('consumer.properties')
        jaas_config = config.get('sasl.jaas.config', '')
        username = jaas_config.split('username=')[1].split(' ')[0].replace('"', '')
        password = jaas_config.split('password=')[1].replace('"', '').replace(';', '')

        self.consumer = Consumer({
            'bootstrap.servers': ','.join(servers),
            'group.id': group_id,
            'enable.auto.commit': auto_commit,
            'auto.offset.reset': offset_reset,
            'security.protocol': config.get('security.protocol', 'PLAINTEXT'),
            'sasl.mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl.username': username,
            'sasl.password': password,
            'fetch.max.bytes': fetch_max_bytes,
            'fetch.min.bytes': fetch_min_bytes,
            'fetch.wait.max.ms': fetch_max_wait_ms
        })

        self.consumer.subscribe(topics)
        print(f"Subscribed to topics: {topics}")

    def consume(self):
        print("Start consuming...")
        while True:
            msg = self.consumer.poll(self.poll_timeout)
            if msg is None:
                print("No message received. Waiting...")
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            now = time.time()
            try:
                msg_value = json.loads(msg.value().decode('utf-8'))
                producer_timestamp = msg_value.get('timestamp')
                index = msg_value.get('index')

                msg_size = None
                for header in (msg.headers() or []):
                    if header[0] == 'size_bytes':
                        msg_size = int(header[1].decode('utf-8'))
                        self.total_bytes += msg_size
                        break

                consumer_delay = now - producer_timestamp if producer_timestamp else None

                self.metrics.append({
                    'index': index,
                    'topic': msg.topic(),
                    'size_bytes': msg_size,
                    'producer_timestamp': producer_timestamp,
                    'consumer_timestamp': now,
                    'consumer_delay_sec': consumer_delay
                })
                self.message_count += 1

                if len(self.metrics) >= self.max_messages:
                    self.save_metrics()
                    self.metrics.clear()
                    self.consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Error processing message: {e}")

    def save_metrics(self):
        print("Saving results...")
        df = pd.DataFrame(self.metrics)
        if df.empty:
            return

        total_time = time.time() - self.start_time
        throughput = (self.total_bytes / 1024 / 1024) / total_time if total_time > 0 else 0

        print(f"Consumed {len(df)} messages across topics: {', '.join(set(df['topic']))}")
        print(f"Total size: {self.total_bytes / 1024:.2f} KB")
        print(f"Elapsed time: {total_time:.2f} sec")
        print(f"Throughput: {throughput:.4f} MB/s")

        if self.output_path:
            try:
                dir_name = os.path.dirname(self.output_path)
                base_name, ext = os.path.splitext(os.path.basename(self.output_path))
        
                # Create timestamp with microseconds
                now = datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M%S") + f"_{now.microsecond:06d}"
        
                # Generate final file names
                final_filename = f"{base_name}_{timestamp}{ext}"
                tmp_file = os.path.join(dir_name, f".tmp_{final_filename}")
                final_file = os.path.join(dir_name, final_filename)

                # Write and rename file
                df.to_csv(tmp_file, index=False)
                os.rename(tmp_file, final_file)
                print(f"Saved final metrics file: {final_file}")
            except Exception as e:
                print(f"[ERROR] Failed to save metrics: {e}")


def str2bool(v):
    return v.lower() in ('yes', 'true', 't', '1')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time Kafka Consumer for Metrics")
    parser.add_argument('--topics', type=str, required=True)
    parser.add_argument('--groupId', type=str, default="group-0-0")
    parser.add_argument('--outputPath', type=str, default="message_metrics.csv")
    parser.add_argument('--uris', type=str, required=True)
    parser.add_argument('--enableAutoCommit', type=str2bool, default=True)
    parser.add_argument('--offsetReset', type=str, default="earliest")
    parser.add_argument('--maxMsg', type=int, default=100)
    parser.add_argument('--pollTimeout', type=int, default=300)
    parser.add_argument('--maxRecords', type=int, default=500)
    parser.add_argument('--fetchMaxBytes', type=int, default=10485760)
    parser.add_argument('--fetchMinBytes', type=int, default=1024)
    parser.add_argument('--fetchMaxWaitMs', type=int, default=500)

    args = parser.parse_args()

    topic_list = args.topics.split(',')
    server_list = [s.strip() for s in args.uris.split(',') if s.strip()]
    auth_config = ConfigLoader.read_properties('consumer.properties')

    consumer = MetricConsumer(
        topics=topic_list,
        servers=server_list,
        group_id=args.groupId,
        output_path=args.outputPath,
        auto_commit=args.enableAutoCommit,
        offset_reset=args.offsetReset,
        auth_config=auth_config,
        max_messages=args.maxMsg,
        poll_timeout=args.pollTimeout,
        max_records=args.maxRecords,
        fetch_max_bytes=args.fetchMaxBytes,
        fetch_min_bytes=args.fetchMinBytes,
        fetch_max_wait_ms=args.fetchMaxWaitMs
    )

    consumer.consume()

