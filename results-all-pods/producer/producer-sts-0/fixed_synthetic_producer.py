import json
import random
import time
import logging
import re
import argparse
import socket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError, KafkaConfigurationError

logging.basicConfig(level=logging.DEBUG)


class Tools:
    def read_config(self, filepath):
        config = {}
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
        return config


class Serializer:
    def str_serializer(self, data):
        return data.encode('utf-8') if isinstance(data, str) else data


class Producer:
    def __init__(self):
        self.hlpr = Tools()
        self.serializer = Serializer()

        self.producer_bootstrap_servers = [
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ]

        # Check connectivity to each broker
        for server in self.producer_bootstrap_servers:
            host, port = server.split(":")
            try:
                ip = socket.gethostbyname(host)
                print(f"DNS lookup successful: {host} resolved to {ip}")
                with socket.create_connection((host, int(port)), timeout=5):
                    print(f"Successfully connected to {host}:{port}")
            except Exception as e:
                print(f"Connection check failed for {host}:{port} — {e}")

        config = self.hlpr.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '').replace('\\', '')
        username_match = re.search(r'username\s*=\s*"([^"]+)"', sasl_config)
        password_match = re.search(r'password\s*=\s*"([^"]+)"', sasl_config)

        username = username_match.group(1) if username_match else ''
        password = password_match.group(1) if password_match else ''
        print(f"Parsed username: {username}")
        print(f"Parsed password: {password}")

        self.admin_config_args = {
            'security_protocol': config.get('security.protocol', 'SASL_PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'SCRAM-SHA-256'),
            'sasl_plain_username': username,
            'sasl_plain_password': password,
        }

        self.producer_config_args = {
            **self.admin_config_args,
            'value_serializer': self.serializer.str_serializer,
            'acks': 1,
            'linger_ms': 100,
            'compression_type': 'lz4',
            'batch_size': 16384,
        }

        self.producer = None
        retry_delay = 5

        while self.producer is None:
            try:
                print("Attempting to connect to Kafka broker...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.producer_bootstrap_servers,
                    **self.producer_config_args
                )
                print("Kafka Producer successfully connected.")
            except NoBrokersAvailable:
                print(f"No brokers available. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            except Exception as ex:
                print(f"Error initializing Kafka Producer: {ex}")
                time.sleep(retry_delay)

    def create_topics_from_configmap(self, configmap_path='./pipeline-configmap.yaml'):
        config_data = {}
        with open(configmap_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                config_data[key.strip()] = value.strip().strip('"')

        topic_count = int(config_data.get("PRODUCER_TOPIC_COUNT", 10))
        topic_prefix = config_data.get("TOPIC_TITLE", "synthetic")
        num_partitions = int(config_data.get("NUM_PARTITIONS", 1))
        replication_factor = int(config_data.get("REPLICATION_FACTOR", "1"))

        print(f"Preparing to create {topic_count} Kafka topics with prefix '{topic_prefix}'")

        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.producer_bootstrap_servers,
                api_version=(3, 8, 1),
                request_timeout_ms=5000,
                metadata_max_age_ms=3000,
                **self.admin_config_args
            )
        except KafkaConfigurationError as kce:
            print(f"Kafka admin client configuration error: {kce}")
            return

        try:
            existing_topics = admin.list_topics()
            print(f"Found {len(existing_topics)} existing topics.")
        except Exception as e:
            print(f"Failed to fetch topics: {e}")
            existing_topics = []

        topics_to_create = [
            NewTopic(name=f"{topic_prefix}_{i}", num_partitions=num_partitions, replication_factor=replication_factor)
            for i in range(topic_count)
            if f"{topic_prefix}_{i}" not in existing_topics
        ]

        if topics_to_create:
            try:
                admin.create_topics(new_topics=topics_to_create, validate_only=False)
                print(f"Created topics: {[t.name for t in topics_to_create]}")
            except TopicAlreadyExistsError:
                print("Some topics already exist.")
            except Exception as e:
                print(f"Error creating topics: {e}")
        else:
            print("All topics already exist.")

        admin.close()

    def send_message_no_flush(self, topic, message, headers=None):
        try:
            future = self.producer.send(topic, value=message, headers=headers or [])
            future.get(timeout=10)
            print(f"Sent message to topic '{topic}'")
        except Exception as e:
            print(f"Error sending message: {e}")

    def send_message(self, topic, message, headers=None):
        try:
            self.producer.send(topic, value=message, headers=headers or [])
            self.producer.flush()
            print(f"Sent message to topic '{topic}'")
        except Exception as e:
            print(f"Error sending message: {e}")

    def start_synthetic_stream(self, num_topics=10, delay=0.5, column_range=500000):
        index = 0
        while True:
            topic = f"synthetic_{index % num_topics}"
            values = [random.randint(0, column_range) for _ in range(random.randint(3, 6))]
            payload = {
                "index": index,
                "timestamp": time.time(),
                "values": values
            }

            message_json = json.dumps(payload)
            message_bytes = message_json.encode('utf-8')
            headers = [
                ('size_bytes', str(len(message_bytes)).encode('utf-8')),
                ('index', str(index).encode('utf-8'))
            ]

            print(f"Sending index {index} to topic {topic}")
            self.send_message_no_flush(topic, message_bytes, headers=headers)
            index += 1
            time.sleep(delay)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Kafka Data Producer")
    parser.add_argument('--topics', type=int, default=10, help='Number of Kafka topics to use')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between messages in seconds')
    parser.add_argument('--configmap', type=str, default='./pipeline-configmap.yaml', help='Path to mounted configmap file')
    args = parser.parse_args()

    producer = Producer()
    producer.create_topics_from_configmap(configmap_path=args.configmap)
    producer.start_synthetic_stream(num_topics=args.topics, delay=args.delay)

