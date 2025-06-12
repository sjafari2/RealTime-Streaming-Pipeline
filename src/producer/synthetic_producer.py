import json
import random
import time
import re
import argparse
import socket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
import logging
#logging.basicConfig(level=logging.DEBUG)


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
        if isinstance(data, str):
            return data.encode('utf-8')
        return data


class Producer:
    def __init__(self):
        self.hlpr = Tools()
        self.serializer = Serializer()

        self.producer_bootstrap_servers = [
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
 ]

        # DNS + Port Reachability Check
        for server in self.producer_bootstrap_servers:
            host, port = server.split(":")
            try:
                ip = socket.gethostbyname(host)
                print(f"DNS lookup successful: {host} resolved to {ip}")
                with socket.create_connection((host, int(port)), timeout=5):
                    print(f"Successfully connected to {host}:{port}")
            except socket.gaierror:
                print(f"DNS resolution failed for {host}")
            except socket.timeout:
                print(f"Connection timed out to {host}:{port}")
            except Exception as e:
                print(f"Failed to connect to {host}:{port} — {e}")

        config = self.hlpr.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '').replace('\\', '')
        username_match = re.search(r'username\s*=\s*"([^"]+)"', sasl_config)
        password_match = re.search(r'password\s*=\s*"([^"]+)"', sasl_config)

        username = username_match.group(1) if username_match else ''
        password = password_match.group(1) if password_match else ''
        print(f"Parsed username: {username}")
        print(f"Parsed password: {password}")
        self.producer_config_args = {
            'security_protocol': config.get('security.protocol', 'SASL_PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl_plain_username': username,
            'sasl_plain_password': password,
            'value_serializer': self.serializer.str_serializer,
            'acks': 1,
            'linger_ms': 100,
            'compression_type': 'lz4',
            'batch_size': 16384,
        }

        self.producer = None
        retry_delay = 5  # seconds

        while self.producer is None:
            try:
                print("Attempting to connect to Kafka broker...")
                self.producer = KafkaProducer(bootstrap_servers=self.producer_bootstrap_servers, **self.producer_config_args)
                print("Kafka Producer successfully connected.")
            except NoBrokersAvailable as e:
                logging.warning("No Kafka brokers available. Retrying in {} seconds...".format(retry_delay))
                print("No brokers available. Retrying in {} seconds...".format(retry_delay))
                time.sleep(retry_delay)
            except Exception as ex:
                logging.error('Unexpected error while creating Kafka Producer: ' + str(ex))
                print('Error initializing Kafka Producer:', str(ex))
                print("Retrying in {} seconds...".format(retry_delay))
                time.sleep(retry_delay)
    '''

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
        replication_factor = int (config_data.get("REPLICATION_FACTOR","1"))

        print(f"Preparing to create {topic_count} Kafka topics with prefix '{topic_prefix}'")

        admin = KafkaAdminClient(
            bootstrap_servers=self.producer_bootstrap_servers,
            security_protocol=self.producer_config_args['security_protocol'],
            sasl_mechanism=self.producer_config_args['sasl_mechanism'],
            sasl_plain_username=self.producer_config_args['sasl_plain_username'],
            sasl_plain_password=self.producer_config_args['sasl_plain_password'],
           # api_version=(1, 6, 1),
            request_timeout_ms=5000,
            metadata_max_age_ms=3000,
            )

        try:
            print("Fetching existing Kafka topics...")
            existing_topics = admin.list_topics()
            print(f"Found {len(existing_topics)} existing topics.")
        except Exception as e:
            print(f"Failed to fetch existing topics: {e}")
            print("Proceeding to create topics unconditionally...")
            existing_topics = []
            

        topics_to_create = []

        for i in range(topic_count):
            topic_name = f"{topic_prefix}_{i}"
            if topic_name not in existing_topics:
                topics_to_create.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))

        if topics_to_create:
            try:
                admin.create_topics(new_topics=topics_to_create, validate_only=False)
                print(f"Created topics: {[t.name for t in topics_to_create]}")
            except TopicAlreadyExistsError:
                print("Some topics already exist.")
            except Exception as e:
                print(f" Error creating topics: {e}")
        else:
            print("All topics already exist.")

        admin.close()
'''
    def send_message_no_flush(self, topic, message, headers=None):
        try:
           #print("Sending message...")
            future = self.producer.send(topic, value=message, headers=headers or [])
            print("Waiting for Kafka to acknowledge...")
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

    def start_synthetic_stream(self, topic_title, num_topics, delay,num_partitions, replica_factor,random_range):
        index = 0
        while True:
            topic = f"{topic_title}_{index % num_topics}"
            values = [random.randint(0, random_range) for _ in range(random.randint(3, 6))]
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

            print(f"Sending message {message_bytes} to topic {topic}")
            self.send_message_no_flush(topic, message_bytes, headers=headers)
            index += 1
            time.sleep(delay)


if __name__ == "__main__":
    print("Start main")
    parser = argparse.ArgumentParser(description="Synthetic Kafka Data Producer")
    parser.add_argument('--topicTitle', type=str, default="synthetic", help='Kafka topics title')
    parser.add_argument('--numTopics', type=int, default=10, help='Number of Kafka topics to use')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between messages in seconds')
    parser.add_argument('--numPartitions', type=int, default=1, help='Nnumber of partitions')
    parser.add_argument('--replica', type=int, default=1, help='Replication factor')
    parser.add_argument('--randomRange', type=int, default=50000, help='Range of random values')
    args = parser.parse_args()
    print("Start Producer")
    producer = Producer()
    producer.start_synthetic_stream(topic_title=args.topicTitle, num_topics=args.numTopics, delay=args.delay, num_partitions=args.numPartitions, replica_factor=args.replica,random_range=args.randomRange)
