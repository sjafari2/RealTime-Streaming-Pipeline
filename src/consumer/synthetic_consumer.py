import json
import time
import argparse
import pandas as pd
from kafka import KafkaConsumer, TopicPartition


class MetricConsumer:
    def __init__(self, topics, servers, group_id, output_path):
        self.topics = topics
        self.output_path = output_path
        self.metrics = []
        self.start_time = time.time()
        self.total_bytes = 0

        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            group_id=group_id,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="user1",
            sasl_plain_password="3qPLZfS1FK"
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
            records = self.consumer.poll(timeout_ms=1000, max_records=100)
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

            if len(self.metrics) >= 100:
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
    print("Start parsing arguments")
    parser = argparse.ArgumentParser(description="Real-time Kafka Consumer for Metrics")
    parser.add_argument('--topics', type=str, required=True, help='Comma-separated Kafka topics to consume from')
    parser.add_argument('--groupId', type=str, default="group-0-0", help='Consumer group id')
    parser.add_argument('--outputPath', type=str, default="message_metrics.csv", help='Path to save metrics')
    parser.add_argument('--uris', type=str, required=True, help='Comma-separated Kafka bootstrap servers')

    args = parser.parse_args()

    topic_list = args.topics.split(',')

    try:
        server_list = [s.strip() for s in args.uris.split(',') if s.strip()]
    except Exception as e:
        print(f"Failed to parse --uris as list: {e}")
        exit(1)

    print("Kafka Servers:", server_list)
    print("Topics:", topic_list)

    consumer = MetricConsumer(
        topics=topic_list,
        servers=server_list,
        group_id=args.groupId,
        output_path=args.outputPath
    )

    consumer.consume()

