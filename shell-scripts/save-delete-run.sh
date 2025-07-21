#!/bin/bash

# CONFIG
NAMESPACE="kafkastreamingdata"
PRODUCER_POD="producer-0"
CONSUMER_POD="consumer-0"
PRODUCER_COUNT=2
CONSUMER_COUNT=2
CHECK_CMD="pgrep -f runsynthetic.sh"
SCRIPT_CMD="/runsynthetic.sh"
LOG_DIR="/mnt/shared/logs"  # Replace with actual mount if needed
MAX_RETRIES=2

# --- Step 0: Check OIDC Token Expiration ---
echo "[INFO] Checking OIDC token expiration..."
TOKEN_PATH="${HOME}/.kube/cache/oidc-login/oidc-token.json"
if [ -f "$TOKEN_PATH" ]; then
    EXPIRY=$(jq -r '.status.expirationTimestamp' "$TOKEN_PATH")
    if [ -n "$EXPIRY" ]; then
        EXPIRY_EPOCH=$(date -d "$EXPIRY" +%s)
        NOW_EPOCH=$(date +%s)
        SECONDS_LEFT=$((EXPIRY_EPOCH - NOW_EPOCH))
        MIN_LEFT=$((SECONDS_LEFT / 60))
        if [ "$MIN_LEFT" -lt 15 ]; then
            echo "[⚠️ WARNING] OIDC token expires in $MIN_LEFT minutes. Consider refreshing:"
            echo "kubectl oidc-login setup --oidc-issuer-url=... --oidc-client-id=..."
        else
            echo "[✅] OIDC token is valid for $MIN_LEFT more minutes."
        fi
    else
        echo "[WARN] Could not parse token expiration."
    fi
else
    echo "[WARN] OIDC token file not found. You might not be authenticated."
fi

# --- Step 1: Ask User to Save & Delete Logs ---
read -p "Do you want to archive logs and results? [y/N]: " save_confirm
if [[ "$save_confirm" =~ ^[Yy]$ ]]; then
    echo "[INFO] Saving logs from $PRODUCER_POD and $CONSUMER_POD..."

    kubectl exec -n "$NAMESPACE" "$PRODUCER_POD" -- bash -c "mkdir -p $LOG_DIR/backup && cp -r $LOG_DIR/* $LOG_DIR/backup/"
    kubectl exec -n "$NAMESPACE" "$CONSUMER_POD" -- bash -c "mkdir -p $LOG_DIR/backup && cp -r $LOG_DIR/* $LOG_DIR/backup/"

    echo "[✅] Logs saved."
fi

read -p "Do you want to delete existing logs/results before running scripts? [y/N]: " delete_confirm
if [[ "$delete_confirm" =~ ^[Yy]$ ]]; then
    echo "[INFO] Deleting logs from shared volume..."

    kubectl exec -n "$NAMESPACE" "$PRODUCER_POD" -- bash -c "rm -rf $LOG_DIR/*"
    kubectl exec -n "$NAMESPACE" "$CONSUMER_POD" -- bash -c "rm -rf $LOG_DIR/*"

    echo "[✅] Logs deleted."
fi

# --- Step 2: Function to Run Script and Confirm It Ran ---
run_and_check() {
    local pod=$1
    local type=$2

    echo "[INFO] Running script in $type pod: $pod"
    attempt=0
    while [ $attempt -lt $MAX_RETRIES ]; do
        kubectl exec -n "$NAMESPACE" "$pod" -- bash -c "$SCRIPT_CMD"
        sleep 2
        echo "[INFO] Checking if script ran on $pod..."
        if kubectl exec -n "$NAMESPACE" "$pod" -- bash -c "$CHECK_CMD" >/dev/null; then
            echo "[✅] Script is running on $pod"
            return 0
        else
            echo "[⚠️] Script not detected on $pod. Retrying... ($((attempt + 1))/$MAX_RETRIES)"
        fi
        attempt=$((attempt + 1))
    done
    echo "[❌] Failed to confirm script ran on $pod after $MAX_RETRIES attempts."
    return 1
}

# --- Step 3: Run in Producer Pods Sequentially ---
echo "[🔁] Starting on producer pods..."
for i in $(seq 0 $((PRODUCER_COUNT - 1))); do
    run_and_check "producer-$i" "producer"
done

# --- Step 4: Run in Consumer Pods Sequentially ---
echo "[🔁] Starting on consumer pods..."
for i in $(seq 0 $((CONSUMER_COUNT - 1))); do
    run_and_check "consumer-$i" "consumer"
done

echo "[✅ DONE] All pods processed."

