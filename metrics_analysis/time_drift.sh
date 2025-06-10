#!/bin/bash
for pod in $(kubectl get pods -n kafkastreamingdata -o name); do
  (
    echo -n "$pod: "
    kubectl exec -n kafkastreamingdata   ${pod#pod/} -- date +%s.%N) &
done

wait

