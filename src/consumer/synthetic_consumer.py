import json
import time
import argparse
import pandas as pd
import helper
from kafka import KafkaConsumer


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
        self.poll_timeout = poll_timeout
        self.max_records = max_records
        self.hlpr = helper.Tools()

        config = self.hlpr.read_config('consumer.properties')
        consumer_security_args = {
            'security_protocol': config.get('security.protocol', 'PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl_plain_username': config.get('sasl.jaas.config').split('username=')[1].split(' ')[0].replace("\"", "").strip(),
            'sasl_plain_password': config.get('sasl.jaas.config').split('password=')[1].replace("\"", "").replace(";", "").strip(),
        }

        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=auto_commit,
            auto_offset_reset=offset_reset,
            group_id=group_id,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            fetch_max_wait_ms=fetch_max_wait_ms,
            **consumer_security_args
        )

        self.consumer.subscribe(topics)
        print("Subscribed to topics. Waiting for partition assignment...")

        for i in range(10):
            partitions = self.consumer.assignment()
            if partitions:
                print(f"Assigned to partitions: {partitions}")
                break
            print(f"Waiting for partition assignment... ({i+1}/10)")
            time.sleep(1)

    def consume(self):
        print("Start consuming...")
        while True:
            records = self.consumer.poll(timeout_ms=self.poll_timeout, max_records=self.max_records)
            if not records:
                print("No new messages. Waiting...")
                time.sleep(1)
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    now = time.time()
                    producer_timestamp = msg.value.get('timestamp')
                    index = msg.value.get('index')
                    delay = now - producer_timestamp if producer_timestamp else None

                    msg_size = None
                    for header in (msg.headers or []):
                        if header[0] == 'size_bytes':
                            msg_size = int(header[1].decode('utf-8'))
                            self.total_bytes += msg_size
                            break

                    self.metrics.append({
                        'index': index,
                        'delay_sec': delay,
                        'size_bytes': msg_size,
                        'recv_timestamp': now,
                        'sent_timestamp': producer_timestamp,
                        'topic': msg.topic
                    })
                    self.message_count += 1

            if len(self.metrics) >= self.max_messages:
                self.save_metrics()
                self.metrics.clear()

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
            df.to_csv(self.output_path, mode='a', index=False, header=not pd.io.common.file_exists(self.output_path))
            print(f"Saved metrics to {self.output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time Kafka Consumer for Metrics")
    parser.add_argument('--topics', type=str, required=True, help='Comma-separated Kafka topics to consume from')
    parser.add_argument('--groupId', type=str, default="group-0-0", help='Consumer group id')
    parser.add_argument('--outputPath', type=str, default="message_metrics.csv", help='Path to save metrics')
    parser.add_argument('--uris', type=str, required=True, help='Comma-separated Kafka bootstrap servers')
    parser.add_argument('--enableAutoCommit', type=bool, default=True, help='Enable auto commit')
    parser.add_argument('--offsetReset', type=str, default="earliest", help='Offset reset policy')
    parser.add_argument('--maxMsg', type=int, default=100, help='Messages per batch before saving to CSV')
    parser.add_argument('--pollTimeout', type=int, default=300, help='KafkaConsumer poll timeout in ms')
    parser.add_argument('--maxRecords', type=int, default=500, help='Maximum records to consume per poll')
    parser.add_argument('--fetchMaxBytes', type=int, default=10485760, help='Maximum bytes fetched per request (default 10MB)')
    parser.add_argument('--fetchMinBytes', type=int, default=1024, help='Minimum bytes to wait for per fetch')
    parser.add_argument('--fetchMaxWaitMs', type=int, default=500, help='Max wait time (ms) before Kafka responds to fetch')

    args = parser.parse_args()

    topic_list = args.topics.split(',')
    server_list = [s.strip() for s in args.uris.split(',') if s.strip()]
    auth_config = ConfigLoader.read_properties('consumer.properties')

    print("Kafka Servers:", server_list)
    print("Topics:", topic_list)

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

