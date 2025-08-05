#!/bin/bash

set -e
NAMESPACE="kafkastreamingdata"

echo "📁 Creating Prometheus config map..."
kubectl create configmap prometheus-config \
  --from-literal=prometheus.yml="$(cat <<EOF
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: 'kafka-producer'
    static_configs:
      - targets: ['producer-sts-0.producer-headless-svc.kafkastreamingdata.svc.cluster.local:8000']
EOF
)" \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

echo "🚀 Deploying Prometheus with resource limits..."
kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
        - name: prometheus
          image: prom/prometheus:v2.52.0
          args:
            - "--config.file=/etc/prometheus/prometheus.yml"
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "256Mi"
      volumes:
        - name: config
          configMap:
            name: prometheus-config
EOF

