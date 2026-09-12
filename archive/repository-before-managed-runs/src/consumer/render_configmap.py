#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
render_configmap.py

Render a Kubernetes ConfigMap from a single experiments YAML file.

Design goals:
- Keep one master file (experiments.yaml) containing:
    base: {...}
    experiments:
      B0: {...}
      S0: {...}
      ...
- Generate a standard ConfigMap with a flat .data map (string -> string),
  compatible with existing env-var-based pipelines.
- Provide clear validation errors for missing EXP_ID profiles.

Usage examples:
  python render_configmap.py --config experiments.yaml --exp-id B0 \
      --name pipeline-configmap --namespace kafkastreamingdata \
      --out pipeline-configmap.yaml

  python render_configmap.py --config experiments.yaml --exp-id S2 | kubectl apply -f -
"""

from __future__ import annotations

import argparse
import sys
from copy import deepcopy
from typing import Any, Dict

import yaml


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge override into base and return a new dict.
    - Scalars/lists in override replace base values.
    - Dicts are merged recursively.
    """
    result = deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = deepcopy(v)
    return result


def flatten_to_string_map(d: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert a dict into a string->string map for ConfigMap .data.
    Rules:
    - None becomes empty string.
    - bool becomes "true"/"false" (lowercase)
    - numbers become their string representation
    - dict/list are emitted as YAML (single string value), so they remain usable
      if you ever need structured values.
    """
    out: Dict[str, str] = {}
    for k, v in d.items():
        if v is None:
            out[str(k)] = ""
        elif isinstance(v, bool):
            out[str(k)] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            # preserve numeric formatting in a readable way
            out[str(k)] = str(v)
        elif isinstance(v, (dict, list)):
            out[str(k)] = yaml.safe_dump(v, default_flow_style=False).strip()
        else:
            out[str(k)] = str(v)
    return out


def load_master_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config file must be a YAML mapping at top level: {path}")
    if "base" not in cfg or "experiments" not in cfg:
        raise ValueError(
            f"Config must contain top-level keys 'base' and 'experiments': {path}"
        )
    if not isinstance(cfg["base"], dict):
        raise ValueError("'base' must be a mapping.")
    if not isinstance(cfg["experiments"], dict):
        raise ValueError("'experiments' must be a mapping of EXP_ID -> overrides.")
    return cfg


def build_configmap(
    merged_data: Dict[str, Any],
    name: str,
    namespace: str,
) -> Dict[str, Any]:
    """
    Build the final Kubernetes ConfigMap object (dict) ready to YAML dump.
    """
    data_str_map = flatten_to_string_map(merged_data)

    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace},
        "data": data_str_map,
    }
    return cm


def main() -> int:
    parser = argparse.ArgumentParser(description="Render ConfigMap from experiments.yaml")
    parser.add_argument("--config", required=True, help="Path to experiments.yaml")
    parser.add_argument("--exp-id", required=True, help="Experiment ID (e.g., B0, S0, S1, S2, K1)")
    parser.add_argument("--name", default="pipeline-configmap", help="ConfigMap name")
    parser.add_argument("--namespace", default=None, help="Kubernetes namespace (overrides config)")
    parser.add_argument("--out", default=None, help="Output path. If omitted, prints to stdout.")
    args = parser.parse_args()

    master = load_master_config(args.config)
    base = master["base"]
    experiments = master["experiments"]

    if args.exp_id not in experiments:
        available = ", ".join(sorted(experiments.keys()))
        print(
            f"ERROR: EXP_ID '{args.exp_id}' not found in {args.config}. "
            f"Available: {available}",
            file=sys.stderr,
        )
        return 2

    merged = deep_merge(base, experiments[args.exp_id])

    # Namespace resolution: CLI overrides config, else use merged NAMESPACE, else default.
    namespace = args.namespace or str(merged.get("NAMESPACE", "default"))

    # Ensure EXP_ID is present (useful for filenames/logging)
    if "EXP_ID" not in merged:
        merged["EXP_ID"] = args.exp_id

    cm_obj = build_configmap(merged_data=merged, name=args.name, namespace=namespace)

    rendered = yaml.safe_dump(cm_obj, sort_keys=False)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(rendered)
    else:
        sys.stdout.write(rendered)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

