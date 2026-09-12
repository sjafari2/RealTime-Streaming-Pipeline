#!/bin/bash

set -e
NAMESPACE="kafkastreamingdata"

echo "🚀 Creating Grafana admin secret..."
kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: grafana-admin-secret
type: Opaque
stringData:
  admin-user: admin
  admin-password: admin123
EOF

echo "📦 Deploying Grafana (no RBAC)..."
kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana
  template:
    metadata:
      labels:
        app: grafana
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:10.3.1
          ports:
            - containerPort: 3000
          env:
            - name: GF_SECURITY_ADMIN_USER
              valueFrom:
                secretKeyRef:
                  name: grafana-admin-secret
                  key: admin-user
            - name: GF_SECURITY_ADMIN_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: grafana-admin-secret
                  key: admin-password
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "256Mi"
EOF

echo "🌐 Creating Grafana service..."
kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: grafana
spec:
  selector:
    app: grafana
  ports:
    - port: 3000
      targetPort: 3000
      protocol: TCP
EOF

echo "⏳ Waiting for Grafana pod to become ready..."
kubectl wait --for=condition=ready pod -l app=grafana -n $NAMESPACE --timeout=180s

echo "🔐 Grafana admin credentials:"
echo "Username: admin"
echo "Password: admin123"

echo "🌍 Port-forwarding Grafana (3000)..."
echo "Access Grafana at: http://localhost:3000"

kubectl port-forward svc/grafana 3000:3000 -n $NAMESPACE

