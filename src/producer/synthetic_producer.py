import json
import random
import time
import re
import argparse
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError, KafkaError

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
    def __init__(self, args):
        self.hlpr = Tools()
        self.serializer = Serializer()
        self.args = args

        self.producer_bootstrap_servers = ["pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092","pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092","pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"]

        config = self.hlpr.read_config('producer.properties')
        try:
            sasl_config = config.get('sasl.jaas.config', '')
            if not sasl_config:
                raise ValueError("sasl.jaas.config is missing in producer.properties")
            username = sasl_config.split('username=')[1].split(' ')[0].strip('"')
            password = sasl_config.split('password=')[1].strip('";')
            self.producer_config_args = {
                'security_protocol': config.get('security.protocol', 'SASL_PLAINTEXT'),
                'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
                'sasl_plain_username': username,
                'sasl_plain_password': password,
                'value_serializer': self.serializer.str_serializer,
                'acks': args.acks,
                'linger_ms': args.lingerMs,
                'compression_type': args.compressionType,
                'batch_size': args.batchSize,
                'max_request_size': args.maxRequestSize,
                'retries': 10,
                'retry_backoff_ms': 2000,
                'request_timeout_ms': 30000,
                'delivery_timeout_ms': 45000
            }
            print(f"Producer config: {self.producer_config_args}")
        except (IndexError, KeyError) as e:
            print(f"Error parsing producer.properties: {e}")
            raise

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.producer_bootstrap_servers,
                **self.producer_config_args
            )
            print("Kafka Producer connected.")
        except Exception as ex:
            print(f"Kafka Producer init error: {ex}")
            raise

        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)
    def create_topics_if_missing(self, base_topic, num_topics, num_partitions, replication_factor):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.producer_bootstrap_servers,
                security_protocol=self.producer_config_args['security_protocol'],
                sasl_mechanism=self.producer_config_args['sasl_mechanism'],
                sasl_plain_username=self.producer_config_args['sasl_plain_username'],
                sasl_plain_password=self.producer_config_args['sasl_plain_password']
            )

            existing_topics = admin.list_topics()
            topics_to_create = []

            for i in range(num_topics):
                topic_name = f"{base_topic}_{i}"
                if topic_name not in existing_topics:
                    topics_to_create.append(NewTopic(
                        name=topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor
                    ))

            if topics_to_create:
                admin.create_topics(new_topics=topics_to_create, validate_only=False)
                print(f"Created topics: {[t.name for t in topics_to_create]}")
            else:
                print("All topics already exist. Skipping creation.")

        except TopicAlreadyExistsError:
            print("Some or all topics already exist.")
        except KafkaError as e:
            print(f"Topic creation failed: {e}")
        except Exception as e:
            print(f"Admin client error: {e}")
        finally:
            if 'admin' in locals():
                admin.close()

    def send_message_no_flush(self, topic, message, headers=None):
        try:
            future = self.producer.send(topic, value=message, headers=headers or [])
            if self.producer_config_args['acks'] != '0':
                future.get(timeout=45)
            print(f"Sent message to topic '{topic}'")
            return True
        except Exception as e:
            import traceback
            print(f"Send error for topic '{topic}': {e}")
            print(f"Stack trace: {traceback.format_exc()}")
            return False

    def start_synthetic_stream(self, topic_title, num_topics, delay, random_range):
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

            if self.send_message_no_flush(topic, message_bytes, headers=headers):
                index += 1
            else:
                print(f"Retrying message for topic '{topic}' after failure")
            time.sleep(delay)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Kafka Data Producer")
    parser.add_argument('--topicTitle', type=str, required=True)
    parser.add_argument('--numTopics', type=int, required=True)
    parser.add_argument('--delay', type=float, required=True)
    parser.add_argument('--numPartitions', type=int, required=True)
    parser.add_argument('--replica', type=int, required=True)
    parser.add_argument('--randomRange', type=int, required=True)
    parser.add_argument('--lingerMs', type=int, required=True)
    parser.add_argument('--compressionType', type=str, required=True)
    parser.add_argument('--batchSize', type=int, required=True)
    parser.add_argument('--maxRequestSize', type=int, required=True)
    parser.add_argument('--acks', type=str, required=True)
    args = parser.parse_args()

    producer = Producer(args)
    try:
        producer.start_synthetic_stream(
            topic_title=args.topicTitle,
            num_topics=args.numTopics,
            delay=args.delay,
            random_range=args.randomRange
        )
    except KeyboardInterrupt:
        print("Stopping producer...")
        producer.producer.close()

