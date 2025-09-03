#!/bin/bash

echo "🔍 Checking node time drift (in seconds.nanoseconds):"

# Get all node names
nodes=$(kubectl get nodes -o name | cut -d'/' -f2)

# Loop over each node
for node in $nodes; do
  echo -n "$node: "
  # Use kubectl debug to run `date` inside the node's host namespace
  kubectl debug node/$node \
    --image=busybox \
    -- chroot /host date +%s.%N 2>/dev/null || echo "❌ Failed"
done

