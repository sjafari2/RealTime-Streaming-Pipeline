#!/usr/bin/env python3
"""
Lag-sensitive Kafka autoscaling controller.

Runs inside a dedicated Kubernetes pod and periodically:
  - measures per-partition lag using kafka-consumer-groups.sh,
  - computes total backlog, skew ratio, and lag growth rate,
  - decides whether to:
        * scale out consumers (broad backlog),
        * attempt targeted reassignment (skewed backlog),
        * escalate to producer-side key splitting,
  - publishes hot-key rules via a Kubernetes ConfigMap.

Configuration:
  All configuration is passed via environment variables, which are populated
  from /config/pipeline-configmap.yaml by run_lag_controller.sh using `yq`,
  exactly like the producer setup.
"""

import os
import time
import json
import subprocess
import logging
from collections import deque, defaultdict

from kubernetes import client, config

# ---------------------------------------------------------------------------
# Helper: get env with optional default
# ---------------------------------------------------------------------------

def getenv_int(name: str, default: int) -> int:
    val = os.getenv(name)
    try:
        return int(val) if val is not None else default
    except ValueError:
        return default

def getenv_float(name: str, default: float) -> float:
    val = os.getenv(name)
    try:
        return float(val) if val is not None else default
    except ValueError:
        return default

# ---------------------------------------------------------------------------
# Load config from environment (exported by run_lag_controller.sh)
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

NAMESPACE = os.getenv("NAMESPACE", "kafkastreamingdata")
RELEASE_NAME = os.getenv("RELEASE_NAME", "pip")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "consgroup")

# This is the consumer StatefulSet name; override via env if needed
CONSUMER_STS_NAME = os.getenv("CONSUMER_STS_NAME", "consumer")

# From pipeline config: target rate is used to set LAG_MIN_THRESHOLD
STEADY_RATE = getenv_int("TARGET_RATE", 1000)

# Path to Kafka CLI (from base image)
KAFKA_INSTALL_PATH = os.getenv("KAFKA_INSTALL_PATH", "/kafka/bin")

# Optional SASL/security properties file, if you use one
KAFKA_CLIENT_CONFIG = os.getenv("KAFKA_CLIENT_CONFIG")

# Controller timing/threshold parameters
DELTA_SECONDS        = getenv_int("DELTA_SECONDS", 15)          # monitoring interval
SKEW_THRESHOLD       = getenv_float("SKEW_THRESHOLD", 10.0)     # skew_ratio threshold
COOLDOWN_SECONDS     = getenv_int("COOLDOWN_SECONDS", 60)       # cooldown after action
WINDOW_SIZE          = getenv_int("WINDOW_SIZE", 4)             # sliding window length
PERSISTENCE_THRESHOLD = getenv_int("PERSISTENCE_THRESHOLD", 3)  # entries needed

EPSILON = 1.0  # avoid division by zero

# LAG_MIN_THRESHOLD ≈ R * Δ (ignore very small backlogs)
LAG_MIN_THRESHOLD    = getenv_int("LAG_MIN_THRESHOLD", STEADY_RATE * DELTA_SECONDS)

# Hot-key splitting parameters
SPLIT_COUNT_DEFAULT  = getenv_int("SPLIT_COUNT", 4)             # typical split_count 2–8
RULE_TTL_SECONDS     = getenv_int("RULE_TTL_SECONDS", 6 * DELTA_SECONDS)
HOTKEY_CONFIGMAP_NAME = os.getenv("HOTKEY_CONFIGMAP_NAME", "hotkey-rules")

# Reassignment retries
MAX_REASSIGN_RETRIES = getenv_int("MAX_REASSIGN_RETRIES", 3)

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("lag_controller")

# ----------------------------------------------------------------------------
# Kubernetes clients
# ----------------------------------------------------------------------------

def init_k8s_clients():
    """Initialize Kubernetes API clients using in-cluster config (or local for debug)."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    apps_v1 = client.AppsV1Api()
    core_v1 = client.CoreV1Api()
    return apps_v1, core_v1

APPS_V1, CORE_V1 = init_k8s_clients()

# ----------------------------------------------------------------------------
# Kafka helpers – lag collection via kafka-consumer-groups.sh
# ----------------------------------------------------------------------------

def get_bootstrap_server() -> str:
    """
    Derive the Kafka bootstrap server.

    Prefer KAFKA_BOOTSTRAP_SERVERS if set; otherwise use the default
    Bitnami-style service name based on RELEASE_NAME and NAMESPACE.
    """
    env_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if env_bootstrap:
        return env_bootstrap

    # Default: <release>-kafka.<namespace>.svc.cluster.local:9092
    return f"{RELEASE_NAME}-kafka.{NAMESPACE}.svc.cluster.local:9092"


def get_partition_lags() -> dict:
    """
    Query Kafka for per-partition lag using kafka-consumer-groups.sh.

    Returns:
        dict[(topic, partition)] -> lag (int)
    """
    bootstrap = get_bootstrap_server()
    kafka_cmd = os.path.join(KAFKA_INSTALL_PATH, "kafka-consumer-groups.sh")

    cmd = [
        kafka_cmd,
        "--bootstrap-server",
        bootstrap,
        "--group",
        CONSUMER_GROUP_ID,
        "--describe",
    ]
    if KAFKA_CLIENT_CONFIG:
        cmd += ["--command-config", KAFKA_CLIENT_CONFIG]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("Failed to run kafka-consumer-groups.sh: %s", e.stderr)
        return {}

    lags = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("GROUP") or line.startswith("Consumer group"):
            continue
        # Format:
        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
        parts = line.split()
        if len(parts) < 6:
            continue
        group, topic, partition, curr_offset, log_end, lag_str = parts[:6]
        try:
            partition = int(partition)
            lag_val = int(lag_str)
        except ValueError:
            continue
        lags[(topic, partition)] = lag_val

    return lags

# ----------------------------------------------------------------------------
# Sliding window and persistence checks
# ----------------------------------------------------------------------------

from collections import deque, defaultdict

class SlidingWindow:
    """Sliding window over recent total_lag and skew_ratio measurements."""

    def __init__(self, size: int):
        self.size = size
        self.total_lag = deque(maxlen=size)
        self.skew_ratio = deque(maxlen=size)

    def add(self, total_lag: int, skew_ratio: float):
        self.total_lag.append(total_lag)
        self.skew_ratio.append(skew_ratio)

    def is_persistent_high_backlog(self) -> bool:
        """High backlog is persistent if most entries exceed LAG_MIN_THRESHOLD."""
        if len(self.total_lag) < self.size:
            return False
        flags = [val >= LAG_MIN_THRESHOLD for val in self.total_lag]
        return sum(flags) >= PERSISTENCE_THRESHOLD

    def is_persistent_skew(self) -> bool:
        """Skew is persistent if most entries exceed SKEW_THRESHOLD."""
        if len(self.skew_ratio) < self.size:
            return False
        flags = [val >= SKEW_THRESHOLD for val in self.skew_ratio]
        return sum(flags) >= PERSISTENCE_THRESHOLD


WINDOW = SlidingWindow(WINDOW_SIZE)
REASSIGN_RETRIES = defaultdict(int)
RULE_VERSION = 0
last_action_time = 0.0

# ----------------------------------------------------------------------------
# Kubernetes actions: scale consumers and publish hot-key rules
# ----------------------------------------------------------------------------

def scale_consumers(delta: int):
    """
    Scale the consumer StatefulSet by delta replicas (delta > 0 for scale-out).
    """
    if delta <= 0:
        logger.info("scale_consumers called with non-positive delta (%d); ignoring.", delta)
        return

    try:
        sts = APPS_V1.read_namespaced_stateful_set(
            name=CONSUMER_STS_NAME,
            namespace=NAMESPACE,
        )
    except client.exceptions.ApiException as e:
        logger.error("Failed to read StatefulSet %s: %s", CONSUMER_STS_NAME, e)
        return

    current_replicas = sts.spec.replicas or 0
    new_replicas = current_replicas + delta

    body = {"spec": {"replicas": new_replicas}}
    try:
        APPS_V1.patch_namespaced_stateful_set(
            name=CONSUMER_STS_NAME,
            namespace=NAMESPACE,
            body=body,
        )
        logger.info("Scaled consumers from %d to %d replicas.", current_replicas, new_replicas)
    except client.exceptions.ApiException as e:
        logger.error("Failed to scale StatefulSet %s: %s", CONSUMER_STS_NAME, e)


def publish_hotkey_rules(key: str, split_count: int, version: int, ttl: int):
    """
    Publish a hot-key splitting rule via a ConfigMap.

    The rule format is:
        {
            "key": key,
            "split_count": split_count,
            "version": version,
            "ttl_seconds": ttl
        }
    Producers periodically read this ConfigMap and apply only the latest rule.
    """
    rule = {
        "key": key,
        "split_count": int(split_count),
        "version": int(version),
        "ttl_seconds": int(ttl),
    }

    data = {"rules.json": json.dumps(rule)}
    metadata = client.V1ObjectMeta(name=HOTKEY_CONFIGMAP_NAME, namespace=NAMESPACE)
    body = client.V1ConfigMap(api_version="v1", kind="ConfigMap", metadata=metadata, data=data)

    try:
        CORE_V1.replace_namespaced_config_map(
            name=HOTKEY_CONFIGMAP_NAME,
            namespace=NAMESPACE,
            body=body,
        )
        logger.info("Updated hotkey ConfigMap %s with rule: %s", HOTKEY_CONFIGMAP_NAME, rule)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            CORE_V1.create_namespaced_config_map(namespace=NAMESPACE, body=body)
            logger.info("Created hotkey ConfigMap %s with rule: %s", HOTKEY_CONFIGMAP_NAME, rule)
        else:
            logger.error("Failed to publish hotkey rules: %s", e)

# ----------------------------------------------------------------------------
# Targeted reassignment (still a stub, as discussed)
# ----------------------------------------------------------------------------

def targeted_reassign(hot_partition, move_quota=1, assignor="cooperative-sticky") -> bool:
    """
    Placeholder for lag-aware targeted reassignment.

    In the current prototype, this function only logs that a reassignment would be
    performed. A full implementation would require consumer-side support (e.g.,
    a control topic or HTTP API) so that consumers can adjust their assignments
    using the Kafka client's 'assign()' call.

    Returns:
        False to indicate no reassignment actually happened.
    """
    logger.info(
        "targeted_reassign called for partition %s (quota=%d, assignor=%s) [stub only]",
        hot_partition,
        move_quota,
        assignor,
    )
    return False

# ----------------------------------------------------------------------------
# Aggregates and hot partition selection
# ----------------------------------------------------------------------------

def compute_aggregates(lags: dict):
    """
    Compute total_lag, max_lag, mean_lag, skew_ratio, and the hot partition key.

    Args:
        lags: dict[(topic, partition)] -> lag

    Returns:
        total_lag (int),
        max_lag (int),
        mean_lag (float),
        skew_ratio (float),
        hot_partition_key (str or None)
    """
    if not lags:
        return 0, 0, 0.0, 0.0, None

    lag_values = list(lags.values())
    total_lag = sum(lag_values)
    max_lag = max(lag_values)
    mean_lag = total_lag / float(len(lag_values))

    skew_ratio = max_lag / (mean_lag + EPSILON)

    hot_topic_partition = max(lags.items(), key=lambda kv: kv[1])[0]
    hot_partition_key = f"{hot_topic_partition[0]}:{hot_topic_partition[1]}"

    return total_lag, max_lag, mean_lag, skew_ratio, hot_partition_key

# ----------------------------------------------------------------------------
# Main control loop
# ----------------------------------------------------------------------------

def main_loop():
    global last_action_time, RULE_VERSION

    last_total_lag = None
    last_time = None

    logger.info(
        "Starting lag-sensitive controller loop (Δ=%ds, SKEW_THRESHOLD=%.1f, LAG_MIN_THRESHOLD=%d)...",
        DELTA_SECONDS,
        SKEW_THRESHOLD,
        LAG_MIN_THRESHOLD,
    )

    while True:
        loop_start = time.time()

        # 1. Measure per-partition lag
        lags = get_partition_lags()
        total_lag, max_lag, mean_lag, skew_ratio, hot_partition_key = compute_aggregates(lags)

        logger.info(
            "Measured total_lag=%d, max_lag=%d, mean_lag=%.2f, skew_ratio=%.2f",
            total_lag,
            max_lag,
            mean_lag,
            skew_ratio,
        )

        # 2. Update sliding window
        WINDOW.add(total_lag, skew_ratio)

        # 3. Ignore very small backlogs
        if total_lag < LAG_MIN_THRESHOLD:
            logger.info(
                "Total lag (%d) below LAG_MIN_THRESHOLD (%d); no action.",
                total_lag,
                LAG_MIN_THRESHOLD,
            )
            time.sleep(max(0, DELTA_SECONDS - (time.time() - loop_start)))
            continue

        now = time.time()

        # 4. Cooldown
        if now - last_action_time < COOLDOWN_SECONDS:
            remaining = COOLDOWN_SECONDS - (now - last_action_time)
            logger.info("In cooldown period (%.1fs remaining); skipping action.", remaining)
            time.sleep(max(0, DELTA_SECONDS - (time.time() - loop_start)))
            continue

        # 5. Lag growth rate (for logging / intuition)
        if last_total_lag is not None and last_time is not None:
            dt = now - last_time
            lag_growth_rate = (total_lag - last_total_lag) / dt if dt > 0 else 0.0
        else:
            lag_growth_rate = 0.0

        last_total_lag = total_lag
        last_time = now

        logger.info("Estimated lag_growth_rate = %.2f messages/sec", lag_growth_rate)

        # 6. Case A: broad backlog -> scale consumers
        if WINDOW.is_persistent_high_backlog() and skew_ratio < SKEW_THRESHOLD:
            logger.info("Persistent high backlog with low skew; scaling consumers.")
            scale_consumers(+1)
            last_action_time = time.time()
            time.sleep(max(0, DELTA_SECONDS - (time.time() - loop_start)))
            continue

        # 7. Case B: skewed backlog (one hot partition dominates)
        if WINDOW.is_persistent_skew():
            if hot_partition_key is None:
                logger.warning("Skew detected but no hot partition identified.")
            else:
                logger.info(
                    "Persistent skew detected (skew_ratio=%.2f); hot partition=%s",
                    skew_ratio,
                    hot_partition_key,
                )

                moved = targeted_reassign(
                    hot_partition=hot_partition_key,
                    move_quota=1,
                    assignor="cooperative-sticky",
                )
                if moved:
                    logger.info("Targeted reassignment applied for %s.", hot_partition_key)
                    last_action_time = time.time()
                    time.sleep(max(0, DELTA_SECONDS - (time.time() - loop_start)))
                    continue

                # Escalation: track retries
                REASSIGN_RETRIES[hot_partition_key] += 1
                attempts = REASSIGN_RETRIES[hot_partition_key]

                if attempts >= MAX_REASSIGN_RETRIES:
                    RULE_VERSION += 1
                    logger.info(
                        "Reassignment retries exhausted for %s (attempts=%d); "
                        "publishing hot-key rules with version=%d.",
                        hot_partition_key,
                        attempts,
                        RULE_VERSION,
                    )
                    publish_hotkey_rules(
                        key=hot_partition_key,
                        split_count=SPLIT_COUNT_DEFAULT,
                        version=RULE_VERSION,
                        ttl=RULE_TTL_SECONDS,
                    )
                    REASSIGN_RETRIES[hot_partition_key] = 0
                    last_action_time = time.time()
                    time.sleep(max(0, DELTA_SECONDS - (time.time() - loop_start)))
                    continue

        # 8. Else: observe
        logger.info("No action taken this interval; continuing to observe.")
        time.sleep(max(0, DELTA_SECONDS - (time.time() - loop_start)))


if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Controller interrupted; exiting.")

