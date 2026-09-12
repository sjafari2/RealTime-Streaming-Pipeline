#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lag-Sensitive Partition-Aware Autoscaling Controller (LS-PAK)

This controller runs as an external Kubernetes microservice.
It:
  - Reads configuration from /config/pipeline-configmap.yaml
  - Connects to Kafka via AdminClient
  - Periodically collects per-partition lag for a target consumer group
  - Classifies the system state (stable / broad pressure / skewed)
  - Applies a least-intrusive-first policy:
        1) Scale consumers (via log/placeholder hook)
        2) Targeted reassignment of hot partitions (placeholder hook)
        3) Publish hot-key bucketing rules (control topic / config)
        4) Suggest partition-count increases
All "actions" are implemented as explicit functions with clear hooks for
integrating with Kubernetes API and real control topics.
"""

import os
import time
import json
import logging
import subprocess
from typing import Dict, List, Tuple

import yaml
from confluent_kafka import AdminClient, KafkaException, TopicPartition


# =========================
# Config and logging
# =========================

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "/config/pipeline-configmap.yaml")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def load_config(path: str) -> Dict[str, str]:
    """
    Load the pipeline configmap YAML and return a flat dict of key -> value.
    Works if the file is a full ConfigMap manifest or a simple key-value YAML.
    """
    with open(path, "r") as f:
        doc = yaml.safe_load(f)

    # If this is a full ConfigMap manifest, use .data
    if isinstance(doc, dict) and "data" in doc:
        data = doc["data"]
    else:
        data = doc

    # Normalize all keys/values to strings
    cfg = {str(k): str(v) for k, v in data.items()}
    return cfg


def build_bootstrap_servers(cfg: Dict[str, str]) -> str:
    """
    Build the Kafka bootstrap.servers string.

    Option 1: Use an explicit env/override if present.
    Option 2: Use your existing naming scheme: pip-kafka.<ns>.svc.cluster.local:9092
    Option 3: Optionally call get_kafka_consumer_dns.sh if mounted.
    """
    # Explicit override
    env_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if env_bootstrap:
        return env_bootstrap

    release = cfg.get("RELEASE_NAME", "pip")
    namespace = cfg.get("NAMESPACE", "kafkastreamingdata")

    # Default Bitnami-style service
    inferred = f"{release}-kafka.{namespace}.svc.cluster.local:9092"

    # Optional: use helper script if available
    helper = "/scripts/get_kafka_consumer_dns.sh"
    if os.path.exists(helper):
        try:
            out = subprocess.check_output([helper], text=True).strip()
            # Expect something like ["host:9092"]
            hosts = json.loads(out)
            if isinstance(hosts, list) and hosts:
                return ",".join(hosts)
        except Exception as e:
            logging.warning("Failed to run %s: %s; falling back to inferred bootstrap %s",
                            helper, e, inferred)

    return inferred


# =========================
# Signals and helpers
# =========================

class Signals:
    """
    Container for metrics at a single snapshot.
    """
    def __init__(self, partition_lag: Dict[Tuple[str, int], int]):
        self.partition_lag = partition_lag

    @property
    def total_lag(self) -> int:
        return sum(self.partition_lag.values())

    @property
    def mean_lag(self) -> float:
        if not self.partition_lag:
            return 0.0
        return self.total_lag / float(len(self.partition_lag))

    @property
    def max_lag_item(self) -> Tuple[Tuple[str, int], int]:
        if not self.partition_lag:
            return (("none", -1), 0)
        k = max(self.partition_lag, key=self.partition_lag.get)
        return k, self.partition_lag[k]

    @property
    def coeff_variation(self) -> float:
        """
        Coefficient of variation (std / mean) as a skew indicator.
        """
        import math

        if not self.partition_lag:
            return 0.0
        mean = self.mean_lag
        if mean <= 0.0:
            return 0.0
        vals = list(self.partition_lag.values())
        var = sum((x - mean) ** 2 for x in vals) / float(len(vals))
        std = math.sqrt(var)
        return std / mean


def persistent(flags: List[bool], min_count: int) -> bool:
    """
    Returns True if 'True' appears at least min_count times in the buffer.
    """
    return sum(1 for f in flags if f) >= min_count


# =========================
# Lag collection from Kafka
# =========================

def get_group_lag(
    admin: AdminClient,
    group_id: str,
    timeout: float = 5.0,
) -> Dict[Tuple[str, int], int]:
    """
    Compute per-partition lag for a consumer group using AdminClient.

    Requires:
      - list_consumer_group_offsets (group offsets)
      - list_offsets (for END offsets)

    Returns:
      dict[(topic, partition)] = lag (>= 0)
    """
    try:
        # 1) Get committed offsets for the group
        grp_offsets = admin.list_consumer_group_offsets(
            group_id,
            request_timeout=timeout,
        )
        tp_to_committed = grp_offsets.result()

        if not tp_to_committed:
            logging.warning("No committed offsets for group %s", group_id)
            return {}

        # 2) Build request for END offsets
        # confluent_kafka expects {TopicPartition: OffsetSpec}
        from confluent_kafka.admin import OffsetSpec

        end_spec = {tp: OffsetSpec.latest() for tp in tp_to_committed.keys()}
        end_offsets = admin.list_offsets(end_spec, request_timeout=timeout)

        partition_lag = {}
        for tp, committed_meta in tp_to_committed.items():
            committed = committed_meta.offset
            if committed < 0:
                # Uninitialized; treat as lag 0 for now
                continue

            end_meta = end_offsets.get(tp, None)
            if end_meta is None or end_meta.offset < 0:
                continue

            lag = max(end_meta.offset - committed, 0)
            partition_lag[(tp.topic, tp.partition)] = lag

        return partition_lag

    except KafkaException as e:
        logging.error("Error computing lag for group %s: %s", group_id, e)
        return {}
    except Exception as e:
        logging.error("Unexpected error in get_group_lag: %s", e)
        return {}


# =========================
# Actions (hooks)
# =========================

def scale_consumers(new_replicas: int):
    """
    Placeholder hook to scale consumer StatefulSet.

    In production:
      - Call Kubernetes API, or
      - Use 'kubectl scale' via subprocess in a pod with proper RBAC.

    Here we just log the intent.
    """
    logging.info("[ACTION] Scale consumers to %d replicas (hook only).", new_replicas)


def targeted_reassign(hot_partitions: List[Tuple[str, int]]):
    """
    Placeholder for targeted reassignment using CooperativeStickyAssignor.

    NOTE:
    Implementing a true external incremental reassignment requires integration with
    Kafka's group management or a custom assignor. For now, we log the intention.
    """
    if not hot_partitions:
        return False
    logging.info("[ACTION] Targeted reassignment for: %s (hook only).", hot_partitions)
    return True


def publish_hotkey_rules(hot_items: List[Tuple[str, int]], control_path: str):
    """
    Write a simple JSON hot-key rule file that producers can watch/mount.

    In a real deployment:
      - This could be a control topic.
      - Or a shared ConfigMap/volume.

    Here:
      - We write to a shared path (if provided).
    """
    if not control_path:
        logging.info("[ACTION] Hot-key rules requested, but no control path configured.")
        return

    rules = {
        "version": int(time.time()),
        "ttl_sec": 300,
        "hot_partitions": [
            {"topic": t, "partition": p, "buckets": 4}
            for (t, p) in hot_items
        ],
    }
    try:
        os.makedirs(os.path.dirname(control_path), exist_ok=True)
        with open(control_path, "w") as f:
            json.dump(rules, f)
        logging.info("[ACTION] Wrote hot-key rules to %s: %s", control_path, rules)
    except Exception as e:
        logging.error("Failed to write hot-key rules to %s: %s", control_path, e)


def maybe_increase_partitions(topic: str, new_count: int):
    """
    Placeholder for increasing topic partitions.

    In production:
      - Use AdminClient.create_partitions.
    Here:
      - Log the intent only.
    """
    logging.info("[ACTION] Suggest increasing partitions for %s to %d (hook only).",
                 topic, new_count)


# =========================
# Controller main loop
# =========================

def run_controller():
    cfg = load_config(CONFIG_PATH)

    group_id = cfg.get("CONSUMER_GROUP_ID", "consgroup")
    topic_title = cfg.get("TOPIC_TITLE", "ae")
    lag_query_interval = int(cfg.get("LAG_QUERY_INTERVAL", "15"))
    lag_query_timeout = float(cfg.get("LAG_QUERY_TIMEOUT", "5.0"))

    # Policy parameters (tune as needed or expose via ConfigMap)
    N = 4  # persistence window (snapshots)
    tau_var = 0.5  # CV threshold for declaring skew
    tau_hot = 0.5  # hot partition holds >= 50% of total lag
    cooldown_sec = 60
    max_retries_before_hotkey = 3

    hotkey_rules_path = os.environ.get(
        "HOTKEY_RULES_PATH",
        "/config/hotkey-rules.json"
    )

    bootstrap_servers = build_bootstrap_servers(cfg)
    logging.info("Using bootstrap.servers=%s", bootstrap_servers)

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # State
    last_action_time = 0.0
    last_state = "stable"
    skew_retry_count = 0

    cv_history: List[float] = []
    broad_flags: List[bool] = []
    skew_flags: List[bool] = []

    # Simple consumer scale bounds
    min_cons = int(cfg.get("CONSUMER_POD_COUNT", "3"))
    max_cons = max(min_cons * 8, 64)

    logging.info("Starting lag-sensitive controller for group=%s topic=%s", group_id, topic_title)

    while True:
        start = time.time()

        # 1) Snapshot metrics
        partition_lag = get_group_lag(admin, group_id, timeout=lag_query_timeout)
        sig = Signals(partition_lag)

        cv = sig.coeff_variation
        cv_history.append(cv)
        if len(cv_history) > N:
            cv_history.pop(0)

        # Define simple conditions
        broad = (cv < tau_var) and (sig.mean_lag > 0)
        skew = False
        hot_items = []
        if sig.total_lag > 0 and sig.partition_lag:
            (hot_tp, hot_lag) = sig.max_lag_item
            if hot_lag / float(sig.total_lag) >= tau_hot:
                skew = True
                hot_items = [hot_tp]

        broad_flags.append(broad)
        skew_flags.append(skew)
        if len(broad_flags) > N:
            broad_flags.pop(0)
        if len(skew_flags) > N:
            skew_flags.pop(0)

        now = time.time()
        if now - last_action_time < cooldown_sec:
            # In cooldown: just log state and wait
            logging.debug("In cooldown; cv=%.3f total_lag=%d", cv, sig.total_lag)
            time.sleep(max(0, lag_query_interval - (time.time() - start)))
            continue

        # Decide state
        state = "stable"
        if persistent(broad_flags, N):
            state = "broad_pressure"
        elif persistent(skew_flags, N):
            state = "skewed_pressure"

        logging.info(
            "Snapshot: state=%s total_lag=%d mean_lag=%.1f cv=%.3f hot=%s",
            state, sig.total_lag, sig.mean_lag, cv,
            hot_items[0] if hot_items else None,
        )

        # 2) Least-intrusive-first

        if state == "stable":
            skew_retry_count = 0

        elif state == "broad_pressure":
            # Scale up consumers slightly
            # In real impl, query current replicas from env/K8s; here we approximate.
            scale_to = min(max_cons, min_cons * 2)
            scale_consumers(scale_to)
            last_action_time = now
            skew_retry_count = 0

        elif state == "skewed_pressure":
            # Try targeted reassignment first (stateless assumption here)
            if hot_items:
                moved = targeted_reassign(hot_items[:1])
                if moved:
                    last_action_time = now
                    skew_retry_count += 1
                else:
                    logging.info("No targeted move performed.")
            else:
                logging.info("Skewed state detected but no hot_items; skipping reassignment.")

            # Escalate to hot-key bucketing if repeated skew
            if skew_retry_count >= max_retries_before_hotkey and hot_items:
                publish_hotkey_rules(hot_items, hotkey_rules_path)
                last_action_time = now
                skew_retry_count = 0

            # Potential further escalation: suggest more partitions if also globally hot
            if sig.total_lag > 0 and sig.mean_lag > 0 and cv > tau_var:
                # Example: suggest doubling partitions (hook only)
                try:
                    cur_parts = int(cfg.get("NUM_PARTITIONS", "72"))
                except ValueError:
                    cur_parts = 72
                maybe_increase_partitions(topic_title, cur_parts * 2)

        last_state = state

        # Sleep until next interval
        elapsed = time.time() - start
        sleep_for = max(0, lag_query_interval - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run_controller()
    except KeyboardInterrupt:
        logging.info("Controller interrupted, shutting down.")
    except Exception as e:
        logging.exception("Fatal error in controller: %s", e)
        raise

