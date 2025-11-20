#!/bin/bash
# upgrade_kafka.sh
# This script upgrades or installs the Bitnami Kafka Helm release.

set -e  # Exit immediately if a command exits with a non-zero status

RELEASE_NAME="pip-kafka"
NAMESPACE="kafkastreamingdata"
VALUES_FILE="helm/values-no-jmx.yaml"
CHART="oci://registry-1.docker.io/bitnamicharts/kafka"
CHART_VERSION="32.0.1"

echo "Upgrading or installing Helm release '$RELEASE_NAME' in namespace '$NAMESPACE'..."
helm upgrade --install "$RELEASE_NAME" \
  "$CHART" \
  --version "$CHART_VERSION" \
  --namespace "$NAMESPACE" \
  -f "$VALUES_FILE"

if [ $? -eq 0 ]; then
  echo "Helm upgrade/install completed successfully!"
else
  echo "Helm upgrade/install failed!"
fi

