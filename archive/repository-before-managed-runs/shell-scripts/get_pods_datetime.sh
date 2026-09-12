#!/usr/bin/env bash
NAMESPACE="kafkastreamingdata"
pods=$(kubectl get pods -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}')
for pod in $pods; do
    (
    echo "[$pod]"
    kubectl exec -n "$NAMESPACE" "$pod" -- date -u +"%Y-%m-%d %H:%M:%S.%N"
    echo "---------------------------------------------"
    ) &
done
wait

