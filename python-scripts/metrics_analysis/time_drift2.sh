#!/bin/bash
#
for pod in $(kubectl get pods -n kafkastreamingdata -o name); do
  local_time=$(date +%s.%N)
  pod_time=$(kubectl exec -n kafkastreamingdata ${pod#pod/} -- date +%s.%N)
  echo "$pod - local: $local_time | pod: $pod_time | drift: $(echo "$pod_time - $local_time" | bc)"
done

