import json
import time
import argparse
import pandas as pd
from kafka import KafkaConsumer
import ast


class MetricConsumer:
    def __init__(self, topics, servers, group_id, output_path):
        self.topics = topics
        self.output_path = output_path
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            group_id=group_id
        )
        self.metrics = []
        self.start_time = time.time()
        self.total_bytes = 0

    def consume(self, max_messages=None):
        count = 0
        for msg in self.consumer:
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

            count += 1
            if max_messages and count >= max_messages:
                break

        self.save_metrics()

    def save_metrics(self):
        df = pd.DataFrame(self.metrics)
        total_time = time.time() - self.start_time
        throughput = (self.total_bytes / 1024 / 1024) / total_time if total_time > 0 else 0

        print(f"Consumed {len(df)} messages across topics: {', '.join(set(df['topic']))}")
        print(f"Total size: {self.total_bytes / 1024:.2f} KB")
        print(f"Elapsed time: {total_time:.2f} sec")
        print(f"Throughput: {throughput:.4f} MB/s")

        if self.output_path:
            df.to_csv(self.output_path, index=False)
            print(f"Saved metrics to {self.output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Kafka Data Consumer")
    parser.add_argument('--topics', type=str, required=True, help='Comma-separated Kafka topics to consume from')
    parser.add_argument('--maxMsg', type=int, default=None, help='Maximum messages to consume (optional)')
    parser.add_argument('--groupId', type=str, default="synthetic-consumer", help='Consumer group id')
    parser.add_argument('--outputPath', type=str, default="message_metrics.xlsx", help='Path to save metrics')
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

    consumer.consume(max_messages=args.maxMsg)

