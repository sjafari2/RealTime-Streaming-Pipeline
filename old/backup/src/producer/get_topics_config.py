from confluent_kafka.admin import AdminClient, ConfigResource

admin = AdminClient({'bootstrap.servers': 'pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092'})

resource = ConfigResource('TOPIC', 'kafka_exp_1')

futures = admin.describe_configs([resource])
for res, future in futures.items():
    try:
        configs = future.result()
        for k, v in configs.items():
            print(f"{k}: {v.value} (default={v.is_default})")
    except Exception as e:
        print(f"Failed to get config: {e}")

