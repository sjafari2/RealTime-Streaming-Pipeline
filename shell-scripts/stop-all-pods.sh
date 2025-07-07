#!/usr/bin/env bash

set -e

echo "=============================================="
echo "Stopping pipeline processes in all Kafka pipeline pods..."
echo "=============================================="

# Define pod labels and containers
pod_labels=("app=producer-sts" "app=consumer-sts" "app=merge-sts")
containers=("producer-container" "consumer-container" "merge-container")

# Define processes to kill per pod type
processes_producer=("runsynthetic.sh" "confluent_kafka_producer.py")
processes_consumer=("runsynthetic.sh" "confluent_consumer.py")
processes_merge=("runsynthetic.sh" "confluent_merge.py")

for i in "${!pod_labels[@]}"; do
    label=${pod_labels[$i]}
    container=${containers[$i]}
    pod_type=$(echo $label | cut -d= -f2)

    # Select which processes to kill based on pod type
    if [[ "$pod_type" == "producer-sts" ]]; then
        processes_to_kill=("${processes_producer[@]}")
    elif [[ "$pod_type" == "consumer-sts" ]]; then
        processes_to_kill=("${processes_consumer[@]}")
    elif [[ "$pod_type" == "merge-sts" ]]; then
        processes_to_kill=("${processes_merge[@]}")
    else
        echo "Unknown pod type: $pod_type. Skipping."
        continue
    fi

    echo "Checking pods for $pod_type..."

    pods=$(kubectl get pods -l $label -o jsonpath='{.items[*].metadata.name}')

    for pod in $pods; do
        echo "Checking pod: $pod ($container)..."

        for proc in "${processes_to_kill[@]}"; do
            echo "Attempting to stop process: $proc in $pod..."
            if kubectl exec -c $container $pod -- pkill -f "$proc" 2>/dev/null; then
                echo "Stopped $proc in $pod."
            else
                echo "No $proc process found or could not stop in $pod."
            fi
        done

    done
done

echo "=============================================="
echo "All stop commands issued for pipeline processes in all pods."
echo "=============================================="

