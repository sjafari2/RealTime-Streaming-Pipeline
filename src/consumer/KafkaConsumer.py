# KafkaConsumer.py
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import helper
import logging
import numpy as np
import time
import json
import pandas as pd
import os

class Consumer:
    def __init__(self, serveruri, pod_index):
        self.consumer_methods = {
            "ukrain": self.ukrain_consume,
            "random": self.delay_tracking_consume,
         #   "fake": self.delay_tracking_consume
        }

        self.serializer = helper.Serializer()
        self.hlpr = helper.Tools()
        self.bootstrap_servers = ["pip-kafka.kafkastreamingdata.svc.cluster.local:9092"]

        print(f"Consumer bootstrap servers: {self.bootstrap_servers}")

        config = self.hlpr.read_config('consumer.properties')
        consumer_security_args = {
            'security_protocol': config.get('security.protocol', 'PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl_plain_username': config.get('sasl.jaas.config').split('username=')[1].split(' ')[0].replace("\"", "").strip(),
            'sasl_plain_password': config.get('sasl.jaas.config').split('password=')[1].replace("\"", "").replace(";", "").strip(),
        }

        consumer_config_args = {
            'value_deserializer': self.serializer.str_deserializer,
            'enable_auto_commit': True,
            'auto_offset_reset': "earliest",
            'api_version': (0, 10),
            'group_id': f"consumer-group-{pod_index}",
            'auto_commit_interval_ms': 10000,
            **consumer_security_args
        }

        try:
            self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers, **consumer_config_args)
        except Exception as ex:
            logging.error('Failed to create Kafka Consumer: ' + str(ex))
            print('❌ Error initializing KafkaConsumer')
            print(str(ex))

    def subscribe_to_topics(self, topics):
        try:
            self.consumer.subscribe(topics)
        except Exception as ex:
            print(f"❌ Failed to subscribe to topics: {ex}")

    def poll(self, timeout_ms, max_records):
        try:
            return self.consumer.poll(timeout_ms, max_records)
        except Exception as ex:
            print(f"❌ Poll error: {ex}")
            return {}

    def assign_to_topic(self, topic):
        try:
            partitions = self.consumer.partitions_for_topic(topic)
            if not partitions:
                print(f"No partitions found for topic: {topic}")
                return
            self.consumer.assign([TopicPartition(topic, p) for p in partitions])
        except Exception as ex:
            print(f"❌ Assign error: {ex}")

    def close(self):
        self.consumer.close()

    def consume_stream(self, topicTitle, topics, **kwargs):
        try:
            self.subscribe_to_topics(topics)
            while True:
                msg = self.poll(5000, 1000)
                if not msg:
                    print("⏳ No new messages. Waiting...")
                    time.sleep(5)
                    continue

                consume_method = self.consumer_methods.get(topicTitle)
                if not consume_method:
                    raise ValueError(f"No consumer method found for topicTitle: {topicTitle}")
                consume_method(msg, **kwargs)

        except Exception as ex:
            print(f"❌ Exception during consume_stream: {ex}")

    def ukrain_consume(self, msg, unique_rows, unique_columns, row_sums_dict, col_range, pod_index, cindex, file_path):
        row, col, data = [], [], []
        np.set_printoptions(threshold=np.inf)

        for tp, ms in msg.items():
            for m in ms:
                msg_value = eval(m.value)
                print(f"Received: {msg_value}")
                msgkey = list(msg_value.keys())[0]
                payload = msg_value[msgkey]
                unique_rows.add(msgkey)

                if isinstance(payload, list):
                    for val in payload:
                        row.append(msgkey)
                        col.append(val)
                        data.append(1)
                        unique_columns.add(val)

        for r, d in zip(row, data):
            row_sums_dict[r] = row_sums_dict.get(r, 0) + d

        filename = f"consumer-{cindex}-pod-{pod_index}"
        try:
            self.hlpr.save_matrix(filename, file_path, col, row, data, col_range)
            print(f"✅ CSR matrix saved to {file_path}/{filename}")
        except Exception as ex:
            print(f"❌ Matrix save error: {ex}")

    def delay_tracking_consume(self, msg, file_path="./delay_logs", **kwargs):
        records = []
        os.makedirs(file_path, exist_ok=True)

        for tp, ms in msg.items():
            for m in ms:
                try:
                    headers = dict(m.headers or [])
                    index = headers.get("index", b"-1").decode("utf-8")
                    message_value = json.loads(m.value)
                    message_key = list(message_value.keys())[0]
                    payload = message_value[message_key]

                    sent_time = float(payload.get("timestamp", 0))
                    recv_time = time.time()
                    delay = recv_time - sent_time

                    records.append({
                        "message_index": index,
                        "message_key": message_key,
                        "sent_time": sent_time,
                        "recv_time": recv_time,
                        "delay_sec": delay
                    })
                except Exception as ex:
                    print(f"⚠️ Failed to process message: {ex}")

        if records:
            df = pd.DataFrame(records)
            timestamp_str = time.strftime("%Y%m%d-%H%M%S")
            output_file = os.path.join(file_path, f"delay_log_{timestamp_str}.xlsx")
            df.to_excel(output_file, index=False)
            print(f"📊 Delay report saved: {output_file}")

