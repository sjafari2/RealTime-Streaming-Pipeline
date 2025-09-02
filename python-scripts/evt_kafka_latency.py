#!/usr/bin/env python3
"""
EVT (GPD Peak-Over-Threshold) analysis for Kafka consumer latency logs.

Input:
  - One or more CSV files containing a column named 'end_to_end_latency_seconds'
  - Files can be provided explicitly, or a directory can be given to load all *.csv within it

What it does:
  1) Loads and concatenates latency data
  2) Chooses a threshold at a given percentile (default: 0.90) or a fixed numeric threshold (seconds)
  3) Fits a Generalized Pareto (GPD) to exceedances using MLE (SciPy) or a fallback estimator
  4) Reports tail metrics:
       - p90/p95/p99/p99.9 estimates (seconds)
       - Probability of exceeding given SLAs (e.g., 0.5s, 1s)
  5) Saves quick diagnostic plots:
       - Tail histogram with fitted survival curve
       - GPD QQ-plot for exceedances

Usage examples:
  python evt_kafka_latency.py --paths /path/to/dir_with_csvs --percentile 0.9
  python evt_kafka_latency.py --paths /path/file1.csv /path/file2.csv --percentile 0.9 --slas 0.5 1.0 2.0

"""
import argparse
import os
import glob
import sys
import math
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Optional SciPy imports for MLE fit
_has_scipy = False
try:
    from scipy.stats import genpareto
    from scipy.optimize import minimize
    _has_scipy = True
except Exception:
    _has_scipy = False


def load_latency_series(paths: List[str], column: str = "end_to_end_latency_seconds") -> pd.Series:
    files = []
    for p in paths:
        if os.path.isdir(p):
            files.extend(sorted(glob.glob(os.path.join(p, "*.csv"))))
        elif os.path.isfile(p):
            files.append(p)
        else:
            print(f"[WARN] Path not found or unsupported: {p}")
    if not files:
        raise FileNotFoundError("No CSV files found from provided paths.")

    series_list = []
    for f in files:
        try:
            df = pd.read_csv(f)
            if column not in df.columns:
                print(f"[WARN] File {f} missing column '{column}', skipping.")
                continue
            s = pd.to_numeric(df[column], errors="coerce").dropna()
            if len(s) == 0:
                print(f"[WARN] File {f} has no valid values in '{column}', skipping.")
                continue
            series_list.append(s)
        except Exception as e:
            print(f"[WARN] Failed to read {f}: {e}")

    if not series_list:
        raise ValueError("No valid data loaded from CSVs.")
    all_lat = pd.concat(series_list, ignore_index=True)
    all_lat = all_lat[all_lat >= 0]  # basic sanity: non-negative latencies
    return all_lat


def choose_threshold(data: pd.Series, percentile: Optional[float], fixed: Optional[float]) -> float:
    if fixed is not None:
        return float(fixed)
    if percentile is None:
        percentile = 0.90
    if not (0 < percentile < 1):
        raise ValueError("--percentile must be in (0,1)")
    return float(np.quantile(data, percentile))


def gpd_fit_mle_scipy(excesses: np.ndarray) -> Tuple[float, float]:
    """Fit GPD to positive excesses (y = x - u > 0) via SciPy MLE."""
    # SciPy's genpareto uses shape=c (xi), loc, scale=beta
    # We'll fix loc=0 for exceedances
    c, loc, scale = genpareto.fit(excesses, floc=0.0)  # let it optimize c and scale
    xi = float(c)
    beta = float(scale)
    return xi, beta


def gpd_fit_fallback(excesses: np.ndarray) -> Tuple[float, float]:
    """Simple fallback estimator if SciPy is not available.
    - If tail seems heavy (xi>0), use Hill estimator for xi and MoM for beta.
    - Otherwise, use method of moments approx.
    References: Pickands/Hill estimators (simplified for robustness here).
    """
    y = np.asarray(excesses, dtype=float)
    y = y[y > 0]
    if len(y) < 50:
        # too few points for stable fit; use crude estimates
        xi = 0.1
        beta = max(np.mean(y), 1e-9)
        return xi, beta

    # Heuristic: assume heavy tail if ratio of mean to median is large
    mean_y = np.mean(y)
    median_y = np.median(y)
    heavy = (mean_y / (median_y + 1e-12)) > 1.5

    if heavy:
        # Hill estimator (requires xi > 0): xi_hat = (1/k) * sum(log(y_i / y_k))
        y_sorted = np.sort(y)
        k = max(int(0.2 * len(y_sorted)), 30)  # top 20% or at least 30 points
        yk = y_sorted[-k]
        logs = np.log(y_sorted[-k:] / (yk + 1e-12))
        xi_hat = np.mean(logs) if np.isfinite(np.mean(logs)) else 0.1
        xi_hat = max(xi_hat, 1e-6)
        beta_hat = mean_y * (1 - xi_hat)  # rough MoM-ish correction
        beta_hat = max(beta_hat, 1e-9)
        return float(xi_hat), float(beta_hat)
    else:
        # Method of moments (works if xi < 0.5)
        m1 = mean_y
        m2 = np.mean((y - m1) ** 2)  # variance
        if m2 <= 0:
            xi_hat = 0.1
            beta_hat = max(m1, 1e-9)
            return float(xi_hat), float(beta_hat)
        xi_hat = 0.5 * (1 - (m1 ** 2) / m2)
        # constrain xi to (-0.5, 0.5) for stability
        xi_hat = float(np.clip(xi_hat, -0.45, 0.45))
        beta_hat = m1 * (1 - xi_hat)
        beta_hat = float(max(beta_hat, 1e-9))
        return xi_hat, beta_hat


def fit_gpd_pot(x: np.ndarray, u: float) -> Tuple[float, float, float]:
    """Fit GPD to exceedances above threshold u. Returns (xi, beta, u)."""
    y = x[x > u] - u
    if len(y) < 30:
        raise ValueError(f"Not enough exceedances above threshold (n={len(y)}). Lower the threshold or gather more data.")
    if _has_scipy:
        xi, beta = gpd_fit_mle_scipy(y)
    else:
        xi, beta = gpd_fit_fallback(y)
    return xi, beta, u


def quantile_from_gpd(u: float, xi: float, beta: float, p: float, p_u: float) -> float:
    """Return overall quantile x_p such that P(X <= x_p) = p, using POT model.
       p_u = F(u) ~ proportion of data <= u (empirical)."""
    if p <= p_u:
        # Within body: fallback to empirical quantile for stability
        return u  # safe lower bound; caller may blend with empirical
    q = (1 - p) / (1 - p_u)  # tail prob conditional
    # Conditional quantile for exceedances: y_q = beta/xi * (q^{-xi} - 1) if xi != 0
    if abs(xi) < 1e-8:
        y_q = beta * math.log(1.0 / q)
    else:
        y_q = (beta / xi) * (q ** (-xi) - 1.0)
    return u + y_q


def exceed_prob_from_gpd(u: float, xi: float, beta: float, s: float, p_u: float) -> float:
    """Return P(X > s)."""
    if s <= u:
        # Use empirical CDF below u
        return 1 - p_u
    y = s - u
    if abs(xi) < 1e-8:
        tail_cond = math.exp(-y / beta)
    else:
        base = 1 + (xi * y) / beta
        if base <= 0:
            return 0.0  # s beyond finite endpoint (for xi < 0) => prob 0
        tail_cond = base ** (-1.0 / xi)
    return (1 - p_u) * tail_cond


def make_plots(x: np.ndarray, u: float, xi: float, beta: float, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    # Tail histogram + fitted survival
    tail = x[x > u]
    y = tail - u
    if len(y) > 0:
        # Empirical survival for exceedances
        ys = np.sort(y)
        emp_sf = 1.0 - np.arange(1, len(ys) + 1) / (len(ys) + 1.0)
        # Fitted survival for exceedances
        def fitted_sf(t):
            if abs(xi) < 1e-8:
                return np.exp(-t / beta)
            base = 1 + (xi * t) / beta
            base = np.maximum(base, 1e-12)
            return np.power(base, -1.0 / xi)

        fig = plt.figure(figsize=(7,5))
        plt.loglog(ys + 1e-12, emp_sf, marker='o', linestyle='None', label='Empirical tail SF')
        grid_t = np.linspace(ys.min(), ys.max(), 200)
        plt.loglog(grid_t + 1e-12, fitted_sf(grid_t), label='Fitted GPD SF')
        plt.xlabel("Exceedance y = x - u (s)")
        plt.ylabel("Survival Function P(Y>y)")
        plt.title("Tail exceedances: empirical vs fitted")
        plt.legend()
        plt.tight_layout()
        fig.savefig(os.path.join(outdir, "tail_survival.png"))
        plt.close(fig)

        # QQ-plot for GPD: compare quantiles
        probs = (np.arange(1, len(ys)+1) - 0.5) / len(ys)
        if abs(xi) < 1e-8:
            theo = -beta * np.log(1 - probs)
        else:
            theo = beta/xi * ((1 - probs)**(-xi) - 1)
        fig2 = plt.figure(figsize=(6,6))
        plt.plot(theo, ys, 'o', alpha=0.6)
        minv = min(theo.min(), ys.min())
        maxv = max(theo.max(), ys.max())
        plt.plot([minv, maxv], [minv, maxv], 'k--', label='y=x')
        plt.xlabel("Theoretical GPD quantiles")
        plt.ylabel("Empirical exceedances")
        plt.title("GPD QQ-plot (exceedances)")
        plt.legend()
        plt.tight_layout()
        fig2.savefig(os.path.join(outdir, "gpd_qqplot.png"))
        plt.close(fig2)


def main():
    ap = argparse.ArgumentParser(description="EVT (GPD POT) analysis for Kafka latency CSVs.")
    ap.add_argument("--paths", nargs="+", required=True, help="CSV files and/or directories containing CSVs.")
    ap.add_argument("--column", default="end_to_end_latency_seconds", help="Latency column name.")
    ap.add_argument("--percentile", type=float, default=0.9, help="Threshold percentile in (0,1). Ignored if --threshold is set.")
    ap.add_argument("--threshold", type=float, default=None, help="Fixed numeric threshold in seconds (optional).")
    ap.add_argument("--slas", type=float, nargs="*", default=[0.5, 1.0], help="SLA thresholds (seconds) to evaluate exceedance probability.")
    ap.add_argument("--outdir", default="evt_output", help="Directory to write plots and report.")
    args = ap.parse_args()

    x = load_latency_series(args.paths, column=args.column).to_numpy()
    n = len(x)
    print(f"[INFO] Loaded {n} latency samples.")

    u = choose_threshold(pd.Series(x), args.percentile, args.threshold)
    p_u = float((x <= u).mean())
    m = (x > u).sum()
    print(f"[INFO] Threshold u = {u:.6f} s | proportion <= u: {p_u:.4f} | exceedances: {m}")

    xi, beta, u = fit_gpd_pot(x, u)
    print(f"[INFO] Fitted GPD params (exceedances): xi={xi:.5f}, beta={beta:.6f}")

    # Report key quantiles (overall)
    for p in [0.90, 0.95, 0.99, 0.999]:
        try:
            q = quantile_from_gpd(u, xi, beta, p, p_u)
            print(f"Estimated p{int(100*p)} latency: {q*1000:.2f} ms")
        except Exception as e:
            print(f"[WARN] Could not compute p{int(100*p)}: {e}")

    # Exceedance probabilities for SLAs
    for s in args.slas:
        try:
            prob = exceed_prob_from_gpd(u, xi, beta, s, p_u)
            print(f"P(latency > {s*1000:.0f} ms) ≈ {prob:.6f} ({prob*100:.4f}%)")
        except Exception as e:
            print(f"[WARN] Could not compute exceed prob for {s}s: {e}")

    # Save diagnostic plots
    make_plots(x, u, xi, beta, args.outdir)

    # Save a small report
    rep_path = os.path.join(args.outdir, "report.txt")
    os.makedirs(args.outdir, exist_ok=True)
    with open(rep_path, "w") as f:
        f.write(f"Samples: {n}\n")
        f.write(f"Threshold u: {u:.9f} s\n")
        f.write(f"Proportion <= u: {p_u:.6f}\n")
        f.write(f"Exceedances: {m}\n")
        f.write(f"GPD xi: {xi:.9f}\n")
        f.write(f"GPD beta: {beta:.9f}\n")
        for p in [0.90, 0.95, 0.99, 0.999]:
            try:
                q = quantile_from_gpd(u, xi, beta, p, p_u)
                f.write(f"p{int(100*p)}: {q:.9f} s\n")
            except Exception as e:
                f.write(f"p{int(100*p)}: ERROR {e}\n")
        for s in args.slas:
            try:
                prob = exceed_prob_from_gpd(u, xi, beta, s, p_u)
                f.write(f"P(X > {s:.6f} s): {prob:.9f}\n")
            except Exception as e:
                f.write(f"P(X > {s:.6f} s): ERROR {e}\n")

    print(f"[INFO] Wrote report and plots to: {args.outdir}")
    print("[DONE]")
    

if __name__ == "__main__":
    main()
