# test_brokers.py

from confluent_kafka.admin import AdminClient

# List of Kafka brokers to test
brokers = [
    "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
    "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
    "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
]

# SASL config
config = {
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'user1',
    'sasl.password': '5x4XjjbPod',
}

# Loop through each broker and test
for broker in brokers:
    print(f"\n🔍 Testing broker: {broker}")
    try:
        admin = AdminClient(dict(config, **{'bootstrap.servers': broker}))
        metadata = admin.list_topics(timeout=5)
        print(f"✅ SUCCESS: {broker} returned {len(metadata.topics)} topics")
    except Exception as e:
        print(f"❌ FAILED: {broker} -> {e}")

