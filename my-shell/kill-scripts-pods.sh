#!/usr/bin/env bash

set -e

echo "=============================================="
echo "Stopping pipeline processes in all Kafka pipeline pods..."
echo "=============================================="

# Define correct pod labels and container names
pod_labels=("app=producer-sts" "app=consumer-sts" "app=merge-sts")
containers=("producer-container" "consumer-container" "merge-container")

# Define processes to kill for each pod type
processes_producer=("run.sh" "producer.py")
processes_consumer=("run.sh" "consumer.py")
processes_merge=("run.sh" "confluent_merge.py")

# Iterate over all pod types
for i in "${!pod_labels[@]}"; do
    label=${pod_labels[$i]}
    container=${containers[$i]}
    pod_type=$(echo $label | cut -d= -f2)

    echo "----------------------------------------------"
    echo "Checking pods with label: $label"
    pods=$(kubectl get pods -l "$label" -o jsonpath='{.items[*].metadata.name}')

    if [[ -z "$pods" ]]; then
        echo "⚠️  No pods found for label: $label. Skipping..."
        continue
    fi

    echo "Found pods: $pods"

    # Select which processes to kill
    if [[ "$pod_type" == "producer-sts" ]]; then
        processes_to_kill=("${processes_producer[@]}")
    elif [[ "$pod_type" == "consumer-sts" ]]; then
        processes_to_kill=("${processes_consumer[@]}")
    elif [[ "$pod_type" == "merge-sts" ]]; then
        processes_to_kill=("${processes_merge[@]}")
    else
        echo "❌ Unknown pod type: $pod_type. Skipping..."
        continue
    fi

    # Loop through each pod and kill relevant processes
    for pod in $pods; do
        echo "🔍 Checking pod: $pod (container: $container)..."
        
        for proc in "${processes_to_kill[@]}"; do
            echo "➡️  Attempting to kill: $proc in $pod..."

            # First, check if process exists
            if kubectl exec -c "$container" "$pod" -- sh -c "ps aux | grep '$proc' | grep -v grep" > /dev/null; then
                # Kill with -9
                kubectl exec -c "$container" "$pod" -- sh -c \
                    "ps aux | grep '$proc' | grep -v grep | awk '{print \$2}' | xargs -r kill -9"
                echo "✅ Killed $proc in $pod."
            else
                echo "⚠️  No running process found for $proc in $pod."
            fi
        done
    done
done

echo "=============================================="
echo "All kill commands issued for pipeline processes."
echo "=============================================="

