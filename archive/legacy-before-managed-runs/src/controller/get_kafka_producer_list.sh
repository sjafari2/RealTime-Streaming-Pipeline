#!/usr/bin/env bash

set -euo pipefail
trap "exit" INT TERM
trap "kill 0" EXIT

# Path to mounted ConfigMap
CONFIG_FILE="/config/pipeline-configmap.yaml"

# Export all config entries from .data with prefix data_
eval $(
  yq eval '.data | to_entries | map("export data_" + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)
# Set the release name and namespace
RELEASE_NAME=${data_RELEASE_NAME}
NAMESPACE=${data_NAMESPACE}
#NAMESPACE=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
#RELEASE_NAME=$(kubectl get pods -n $NAMESPACE -o=jsonpath='{.items[0].metadata.labels.helm\.sh/release}')

# Number of Kafka brokers (replicas)
BROKER_COUNT=${data_BROKER_COUNT}

# Initialize an empty array to hold DNS names
dns_names=()

# Construct the DNS names and add them to the array
for ((i=0; i<$BROKER_COUNT; i++)); do
    DNS_NAME="${RELEASE_NAME}-kafka-controller-${i}.${RELEASE_NAME}-kafka-controller-headless.${NAMESPACE}.svc.cluster.local:9092"
    dns_names+=("$DNS_NAME")
done

echo $(IFS=,; echo "${dns_names[*]}")
