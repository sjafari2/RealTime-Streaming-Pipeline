#!/usr/bin/env bash

# Exit on any error
set -e

NAMESPACE="kafkastreamingdata"

# Find Prometheus and Grafana services
#PROM_SERVICE=$(kubectl get svc -n $NAMESPACE -o jsonpath='{.items[?(@.metadata.name contains "prometheus")].metadata.name}')
#GRAFANA_SERVICE=$(kubectl get svc -n $NAMESPACE -o jsonpath='{.items[?(@.metadata.name contains "grafana")].metadata.name}')

PROM_SERVICE="prometheus-svc"
GRAFANA_SERVICE="grafana-svc"

if [ -z "$PROM_SERVICE" ] || [ -z "$GRAFANA_SERVICE" ]; then
  echo "Prometheus or Grafana service not found in namespace '$NAMESPACE'"
  exit 1
fi

echo "Found Prometheus service: $PROM_SERVICE"
echo "Found Grafana service: $GRAFANA_SERVICE"

# Port forward Prometheus
echo "Starting port-forward for Prometheus on localhost:9090..."
kubectl port-forward svc/"$PROM_SERVICE" 9090:9090 -n "$NAMESPACE" > /dev/null 2>&1 &
PROM_PID=$!

# Port forward Grafana
echo "Starting port-forward for Grafana on localhost:3000..."
kubectl port-forward svc/"$GRAFANA_SERVICE" 3000:3000 -n "$NAMESPACE" > /dev/null 2>&1 &
GRAF_PID=$!

# Wait a bit to ensure services are up
sleep 3

# Open both in browser
echo "Opening Prometheus and Grafana in your browser..."
if command -v xdg-open >/dev/null; then
  xdg-open http://localhost:9090
  xdg-open http://localhost:3000
elif command -v open >/dev/null; then
  open http://localhost:9090
  open http://localhost:3000
else
  echo "[Open http://localhost:9090 (Prometheus) and http://localhost:3000 (Grafana) in your browser manually."
fi

# Wait for user to exit
echo ""
echo "Press Ctrl+C to stop port-forwarding..."
wait $PROM_PID $GRAF_PID

