# -*- coding: utf-8 -*-
"""
latency_evt_workflow.py

Thin orchestration that *reuses* functions from latency_evt_tools_clean.py when present.
Units: seconds (block_max_value is in seconds).
Outlier threshold default: 1.0 second.
"""

from __future__ import annotations

import os
import yaml
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
import importlib
import numpy as np
from scipy import stats

def ks_truncated_body_gev(values, params, u):
    """
    KS test for body-only (X <= u) against a GEV truncated at u.
    values: raw maxima; we select values <= u inside.
    params: dict with c, loc, scale (fitted on full data).
    u: threshold in seconds.
    """
    x = np.asarray(values, float)
    x = x[np.isfinite(x) & (x <= u)]
    n = x.size
    if n < 5 or params is None:
        return dict(stat=np.nan, pvalue=np.nan, n=n)

    c, loc, scale = params["c"], params["loc"], params["scale"]
    Fu = stats.genextreme.cdf(u, c=c, loc=loc, scale=scale)

    # Truncated CDF: F_tr(x) = F(x)/F(u) for x <= u
    def cdf_truncated(t):
        Ft = stats.genextreme.cdf(t, c=c, loc=loc, scale=scale)
        return np.clip(Ft / Fu, 0.0, 1.0)

    stat, p = stats.kstest(x, cdf_truncated)
    return dict(stat=float(stat), pvalue=float(p), n=int(n))


# Import user's helper; fall back only when a function is missing
_evt = importlib.import_module("latency_evt_tools_clean")

# -----------------------------
# Optional label overrides
# -----------------------------
LABEL_OVERRIDES: Dict[str, str] = {}

def set_label_overrides(mapping: Dict[str, str]) -> None:
    """Set path -> label overrides (e.g., to include POLL_TIMEOUT in ms)."""
    global LABEL_OVERRIDES
    LABEL_OVERRIDES = dict(mapping or {})

# -----------------------------
# Safe call wrappers
# -----------------------------

def _call(name: str, fallback, *args, **kwargs):
    fn = getattr(_evt, name, None)
    if callable(fn):
        return fn(*args, **kwargs)
    return fallback(*args, **kwargs)

def _fallback_read_csv(path: str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def _fallback_parse_time(s: str) -> pd.Timestamp:
    return pd.to_datetime(s, utc=True, errors="coerce")

def _fallback_fit_gev(values: np.ndarray) -> Dict[str, float]:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if values.size < 5:
        return dict(c=0.0, loc=float(np.nanmedian(values) if values.size else 0.0), scale=float(np.nanstd(values) if values.size else 1.0))
    c, loc, scale = stats.genextreme.fit(values)
    scale = max(scale, 1e-9)
    return dict(c=c, loc=loc, scale=scale)

def _fallback_fit_gpd(exceedances: np.ndarray) -> Optional[Dict[str, float]]:
    x = np.asarray(exceedances, dtype=float)
    x = x[np.isfinite(x)]
    if x.size < 20:
        return None
    c, loc, scale = stats.genpareto.fit(x, floc=0.0)
    scale = max(scale, 1e-12)
    return dict(c=c, loc=0.0, scale=scale)

def _fallback_ecdf(x: np.ndarray):
    x = np.sort(x)
    n = x.size
    y = np.arange(1, n + 1, dtype=float) / float(n)
    return x, y

def _fallback_comp_ccdf(x: np.ndarray):
    xs, ys = _fallback_ecdf(x)
    return xs, 1.0 - ys

def _fallback_estimate_throughput(df_bm: pd.DataFrame) -> float:
    # Robust throughput: use span if reasonable; otherwise use median delta
    if "computed_at" not in df_bm.columns or "rows_in_block" not in df_bm.columns:
        return float("nan")
    ts = pd.to_datetime(df_bm["computed_at"], utc=True, errors="coerce").sort_values()
    if ts.isna().all():
        return float("nan")
    tmin, tmax = ts.min(), ts.max()
    total_rows = pd.to_numeric(df_bm["rows_in_block"], errors="coerce").fillna(0).sum()
    span = (tmax - tmin).total_seconds()
    if span and span >= 10.0:
        return float(total_rows) / float(span)
    diffs = ts.diff().dropna().dt.total_seconds()
    med = diffs[diffs > 0].median() if not diffs.empty else None
    if med and med > 0:
        approx_span = max(med * len(ts), 1.0)
        return float(total_rows) / float(approx_span)
    approx_span = max(len(ts), 1.0)
    return float(total_rows) / approx_span

# Map to user helpers or fallbacks
read_csv            = lambda p: _call("read_csv_safe", _fallback_read_csv, p)
parse_time          = lambda s: _call("parse_time", _fallback_parse_time, s)
fit_gev             = lambda v: _call("fit_gev", _fallback_fit_gev, v)
fit_gpd             = lambda v: _call("fit_gpd", _fallback_fit_gpd, v)
ecdf                = lambda v: _call("ecdf", _fallback_ecdf, v)
comp_ccdf           = lambda v: _call("comp_ccdf", _fallback_comp_ccdf, v)
estimate_throughput = lambda df: _call("estimate_throughput", _fallback_estimate_throughput, df)

# -----------------------------
# Data loading and labeling
# -----------------------------

def infer_label_from_config(exp_dir: str) -> str:
    """
    If pipeline-configmap.yaml exists and includes POLL_TIMEOUT, return 'POLL_TIMEOUT=<val>'.
    Otherwise, return the folder name. Path-based overrides win.
    """
    if exp_dir in LABEL_OVERRIDES:
        return LABEL_OVERRIDES[exp_dir]

    cfg_path = os.path.join(exp_dir, "pipeline-configmap.yaml")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            poll = None
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], dict):
                    poll = data["data"].get("POLL_TIMEOUT")
                if poll is None:
                    poll = data.get("POLL_TIMEOUT")
            if poll is not None:
                return f"POLL_TIMEOUT={poll}"
        except Exception:
            pass
    return Path(exp_dir).name

def load_block_maxima(exp_dir: str) -> pd.DataFrame:
    """
    Load block_maxima.csv and validate required columns.
    Columns expected:
      file_name, computed_at, metric, scope, pod, block_max_value, rows_in_block, config_hash
    """
    path = os.path.join(exp_dir, "block_maxima.csv")
    df = read_csv(path)
    if df is None or df.empty:
        raise FileNotFoundError(f"Missing or empty block_maxima.csv in {exp_dir}")
    required = {"file_name","computed_at","metric","scope","pod","block_max_value","rows_in_block","config_hash"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"block_maxima.csv missing columns: {sorted(missing)}")
    df["block_max_value"] = pd.to_numeric(df["block_max_value"], errors="coerce")
    df["rows_in_block"]   = pd.to_numeric(df["rows_in_block"], errors="coerce")
    return df

def load_gev_params_if_any(exp_dir: str) -> Optional[Dict[str, float]]:
    """
    Load GEV parameters from gev_fit_results.csv if present.
    Uses the last row as the current fit.
    """
    path = os.path.join(exp_dir, "gev_fit_results.csv")
    df = read_csv(path)
    if df is None or df.empty:
        return None
    if not set(["shape_c", "location_mu", "scale_beta"]).issubset(df.columns):
        return None
    last = df.tail(1).iloc[0]
    try:
        return dict(c=float(last["shape_c"]),
                    loc=float(last["location_mu"]),
                    scale=max(float(last["scale_beta"]), 1e-9))
    except Exception:
        return None

# -----------------------------
# Core analysis (seconds)
# -----------------------------

def analyze_experiment(exp_dir: str, threshold_s: float = 1.0) -> Dict:
    label = infer_label_from_config(exp_dir)
    df_bm = load_block_maxima(exp_dir).copy()

    if "computed_at" in df_bm.columns:
        df_bm["computed_at_ts"] = pd.to_datetime(df_bm["computed_at"], utc=True, errors="coerce")
    else:
        df_bm["computed_at_ts"] = pd.NaT

    maxima = df_bm["block_max_value"].to_numpy(dtype=float)

    gev_params = load_gev_params_if_any(exp_dir)
    if gev_params is None:
        gev_params = fit_gev(maxima)

    throughput_rps = estimate_throughput(df_bm)

    exceed_mask = maxima > threshold_s
    exceedances = maxima[exceed_mask] - threshold_s
    gpd_params = fit_gpd(exceedances)

    return dict(
        label=label,
        exp_dir=exp_dir,
        df_bm=df_bm,
        maxima=maxima,
        throughput_rps=throughput_rps,
        threshold_s=threshold_s,
        exceedances=exceedances,
        gev_params=gev_params,
        gpd_params=gpd_params,
    )

# -----------------------------
# Plotting – save and show
# -----------------------------

def _safe_legend(ax):
    handles, labels = ax.get_legend_handles_labels()
    if handles and any(l and not str(l).startswith('_') for l in labels):
        ax.legend()

def plot_hist_with_gev(exps: List[Dict], out_png: str, show: bool = True) -> None:
    fig, ax = plt.subplots()
    # Overlaid histograms
    for e in exps:
        vals = e["maxima"]
        if vals.size == 0:
            continue
        ax.hist(vals, bins=50, density=True, histtype="step", label=f"{e['label']} hist")

    # GEV PDFs
    valid = [ex["maxima"] for ex in exps if ex["maxima"].size > 0]
    if valid:
        vmin = float(np.nanmin([np.nanmin(v) for v in valid]))
        vmax = float(np.nanmax([np.nanmax(v) for v in valid]))
        if np.isfinite(vmin) and np.isfinite(vmax) and vmax > vmin:
            xs = np.linspace(vmin, vmax, 400)
            for e in exps:
                p = e["gev_params"]
                if p is None:
                    continue
                pdf = stats.genextreme.pdf(xs, c=p["c"], loc=p["loc"], scale=p["scale"])
                ax.plot(xs, pdf, label=f"{e['label']} GEV")

    ax.set_xlabel("Block Maxima (s)")
    ax.set_ylabel("Density")
    ax.set_title("Block Maxima with GEV overlays (seconds)")
    _safe_legend(ax)
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show:
        plt.show()
    plt.close(fig)

def plot_tail_ccdf(exps: List[Dict], out_png: str, show: bool = True) -> None:
    fig, ax = plt.subplots()
    for e in exps:
        vals = e["maxima"]
        if vals.size == 0:
            continue
        xs, ccdf = comp_ccdf(vals)
        ax.plot(xs, ccdf, label=f"{e['label']} 1-ECDF")
    ax.set_yscale("log")
    ax.set_xlabel("Block Maxima (s)")
    ax.set_ylabel("1 - ECDF")
    ax.set_title("Tail view (complementary CDF) – seconds")
    _safe_legend(ax)
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show:
        plt.show()
    plt.close(fig)

def plot_outlier_ecdf_with_gpd(exps: List[Dict], out_png: str, show: bool = True) -> None:
    fig, ax = plt.subplots()
    any_data = False
    for e in exps:
        thr = e["threshold_s"]
        exc = e["exceedances"]
        if exc.size < 1:
            continue
        xs, ys = ecdf(exc + thr)
        ax.plot(xs, ys, label=f"{e['label']} ECDF (> {thr:.2f} s)")
        any_data = True

    for e in exps:
        params = e["gpd_params"]
        if params is None:
            continue
        thr = e["threshold_s"]
        grid = np.linspace(0, max(1.0, float(np.max(e["exceedances"]))), 300)
        cdf = stats.genpareto.cdf(grid, c=params["c"], loc=params["loc"], scale=params["scale"])
        ax.plot(grid + thr, cdf, linestyle="-.", label=f"{e['label']} GPD")

    if not any_data:
        ax.text(0.5, 0.5, "No exceedances above the threshold.", ha="center", va="center", transform=ax.transAxes)

    ax.set_xlabel("Latency (s)")
    ax.set_ylabel("CDF")
    ax.set_title("Outliers (> threshold, seconds) – ECDF and GPD")
    _safe_legend(ax)
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show:
        plt.show()
    plt.close(fig)

def plot_throughput_bars(exps: List[Dict], out_png: str, show: bool = True) -> None:
    labels = [e["label"] for e in exps]
    vals = [e["throughput_rps"] for e in exps]

    fig, ax = plt.subplots()
    x = np.arange(len(labels))
    ax.bar(x, vals)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("Messages per second")
    ax.set_title("Throughput per experiment")
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show:
        plt.show()
    plt.close(fig)

# -----------------------------
# Summary
# -----------------------------

def save_summary(exps: List[Dict], out_dir: str) -> str:
    rows = []
    for e in exps:
        gp = e["gev_params"] or {}
        pp = e["gpd_params"] or {}
        rows.append(dict(
            label=e["label"],
            exp_dir=e["exp_dir"],
            n_blocks=int(e["maxima"].size),
            throughput_rows_per_sec=e["throughput_rps"],
            gev_c=gp.get("c"),
            gev_loc=gp.get("loc"),
            gev_scale=gp.get("scale"),
            outlier_threshold_s=e["threshold_s"],
            n_exceedances=int(e["exceedances"].size),
            gpd_c=pp.get("c"),
            gpd_loc=pp.get("loc"),
            gpd_scale=pp.get("scale"),
        ))
    df = pd.DataFrame(rows)
    path = os.path.join(out_dir, "gev_gpd_summary.csv")
    df.to_csv(path, index=False)
    return path
