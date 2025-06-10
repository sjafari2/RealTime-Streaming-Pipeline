#!/bin/bash

# Set variables
HOME="/Users/soheila/PycharmProjects/PipelineProject/RealTime-Streaming-Pipeline"
RELEASE_NAME="pip-kafka"
NAMESPACE="kafkastreamingdata"
VALUES_FILE=$HOME"/helm/updated_values.yaml"
PRODUCER_CONFIG=$HOME"/src/producer/producer.properties"
CONSUMER_CONFIG=$HOME"/src/consumer/consumer.properties"


# Step 2: Get Kafka client password from Kubernetes secret
echo "Retrieving Kafka client password from secret..."
SECRET_NAME=$(kubectl get secret -n "$NAMESPACE" -l app.kubernetes.io/instance=$RELEASE_NAME,app.kubernetes.io/name=kafka -o jsonpath="{.items[0].metadata.name}")

if [ -z "$SECRET_NAME" ]; then
    echo "Could not find the Kafka secret in namespace $NAMESPACE"
    exit 1
fi

PASSWORD=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath="{.data.kafka-passwords}" | base64 --decode | grep user1 | awk -F ':' '{print $2}' | tr -d ' ')

if [ -z "$PASSWORD" ]; then
    echo  "Could not extract user1 password from the secret."
    exit 1
fi

# Step 3: Update producer.properties and consumer.properties
echo "Updating Kafka client password in properties files..."
sed -i "s/password=\".*\"/password=\"$PASSWORD\"/" "$PRODUCER_CONFIG"
sed -i "s/password=\".*\"/password=\"$PASSWORD\"/" "$CONSUMER_CONFIG"

echo "Helm  password updated in config files."
