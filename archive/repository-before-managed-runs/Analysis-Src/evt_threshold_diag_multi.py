# evt_threshold_diag_multi.py
# Supports multiple root directories with inner folders (recursive).
# - You can pass a list of roots; we will recursively gather files matching PATTERN from each.
# - Limit how many files per root with MAX_FILES_PER_DIR (None = all).
# - Optionally run per-root diagnostics as well as a combined one.
#
# Uses the same simple inline logic as before (no extra modules).

import os, glob, math, json
from datetime import datetime
from typing import List, Optional, Tuple, Sequence

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import genpareto, kstest

CAND_COLS_S = ['e2e_sec', 'latency_sec', 'e2e_s', 'latency_s']
CAND_COLS_MS = ['e2e_ms', 'latency_ms']

def _coerce_seconds(df: pd.DataFrame):
    for c in CAND_COLS_S:
        if c in df.columns:
            arr = df[c].to_numpy(dtype=float)
            return arr[np.isfinite(arr)]
    for c in CAND_COLS_MS:
        if c in df.columns:
            arr = df[c].to_numpy(dtype=float) / 1000.0
            return arr[np.isfinite(arr)]
        for c in df.columns:
            if pd.api.types.is_numeric_dtype(df[c]):
                arr_raw = df[c].to_numpy(dtype=float)
                arr = arr_raw[np.isfinite(arr_raw)]
                if arr.size == 0:
                    continue
            # Heuristic: if typical value looks like milliseconds, convert to seconds
                try:
                    med = np.nanmedian(arr)
                except Exception:
                    med = np.nan
                if np.isfinite(med) and med > 200:
                    arr = arr / 1000.0
                return arr
    return None

def _collect_files(root: str, pattern: str, recursive: bool = True) -> List[str]:
    if recursive:
        globpat = os.path.join(root, "**", pattern)
        return sorted(glob.glob(globpat, recursive=True))
    else:
        globpat = os.path.join(root, pattern)
        return sorted(glob.glob(globpat, recursive=False))

def load_latencies_multi(
    roots: Sequence[str],
    pattern: str = "*.parquet",
    recursive: bool = True,
    max_files_per_root: Optional[int] = None,
    warmup_frac: float = 0.0,
    keep_warmup: bool = False
) -> np.ndarray:
    vals = []
    for r in roots:
        files = _collect_files(r, pattern, recursive=recursive)
        if max_files_per_root is not None:
            files = files[:max_files_per_root]
        for p in files:
            if p.lower().endswith((".parquet", ".pq")):
                df = pd.read_parquet(p)
            else:
                df = pd.read_csv(p)
            arr = _coerce_seconds(df)
            if arr is not None:
                vals.append(arr)
    if not vals:
        raise FileNotFoundError(f"No matching files found under roots={list(roots)} with pattern={pattern}")
    x = np.sort(np.concatenate(vals))
    if warmup_frac > 0 and not keep_warmup:
        n = len(x); cut = int(n * warmup_frac)
        if 0 < cut < n: x = x[cut:]
    return x

def exceedances(x: np.ndarray, u: float) -> np.ndarray:
    return x[x > u] - u

def gpd_fit(y: np.ndarray) -> Tuple[float, float]:
    c, loc, scale = genpareto.fit(y, floc=0.0)
    return float(c), float(scale)

def ks_test_gpd(y: np.ndarray, xi: float, sigma: float):
    cdf = lambda t: genpareto.cdf(t, c=xi, loc=0.0, scale=sigma)
    return kstest(y, cdf)

def anderson_darling_gof_uniform(uvals: np.ndarray) -> float:
    u = np.sort(np.clip(uvals, 1e-12, 1-1e-12)); n = len(u)
    if n == 0: return float("inf")
    i = np.arange(1, n+1)
    s = np.sum((2*i-1) * (np.log(u) + np.log(1 - u[::-1])))
    return float(-n - s / n)

def ad_test_gpd(y: np.ndarray, xi: float, sigma: float) -> float:
    U = genpareto.cdf(y, c=xi, loc=0.0, scale=sigma)
    return anderson_darling_gof_uniform(U)

def gpd_qq_plot(y: np.ndarray, xi: float, sigma: float, title: str, out_path: str):
    n = len(y)
    if n < 5: return
    y_sorted = np.sort(y)
    probs = (np.arange(1, n+1) - 0.5) / n
    q_theory = genpareto.ppf(probs, c=xi, loc=0.0, scale=sigma)
    plt.figure(figsize=(9, 6))
    plt.scatter(q_theory, y_sorted, s=20)
    lim = max(q_theory.max(), y_sorted.max())
    plt.plot([0, lim], [0, lim])
    plt.xlabel("Theoretical quantiles"); plt.ylabel("Empirical quantiles")
    plt.title(title); plt.tight_layout()
    plt.savefig(out_path, dpi=150); plt.close()

def run_threshold_diagnostics(latencies_s: np.ndarray, thresholds_s: List[float],
                              out_dir: Optional[str] = None,
                              max_points_per_u: Optional[int] = None) -> pd.DataFrame:
    if out_dir is None:
        out_dir = os.path.join("/mnt/data", f"evt_diag_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(out_dir, exist_ok=True)

    # MRL
    mrl_us = np.linspace(min(thresholds_s), max(thresholds_s), 25)
    mrl_vals = []
    for u in mrl_us:
        y = exceedances(latencies_s, u)
        if max_points_per_u and len(y) > max_points_per_u: y = y[:max_points_per_u]
        mrl_vals.append(np.mean(y) if len(y) else np.nan)
    plt.figure(figsize=(9, 6))
    plt.plot(mrl_us, mrl_vals, marker='o')
    plt.xlabel("Threshold u (s)"); plt.ylabel("MRL E[X-u | X>u] (s)")
    plt.title("Mean Residual Life (MRL)"); plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "mrl_curve.png"), dpi=150); plt.close()

    rows, xi_us, xi_vals = [], [], []
    for u in thresholds_s:
        y = exceedances(latencies_s, u); n_exc = len(y)
        if n_exc == 0:
            rows.append(dict(u=u, n_exc=0, xi=np.nan, sigma=np.nan, ks_stat=np.nan, ks_p=np.nan, ad_stat=np.nan))
            continue
        if max_points_per_u and n_exc > max_points_per_u:
            y = y[:max_points_per_u]; n_exc = len(y)
        xi, sigma = gpd_fit(y); ks_stat, ks_p = ks_test_gpd(y, xi, sigma); ad_stat = ad_test_gpd(y, xi, sigma)
        xi_us.append(u); xi_vals.append(xi)
        gpd_qq_plot(y, xi, sigma, f"GPD QQ — u={u:.2f}s (n={n_exc})",
                    os.path.join(out_dir, f"qq_gpd_u_{str(u).replace('.','p')}.png"))
        rows.append(dict(u=u, n_exc=n_exc, xi=xi, sigma=sigma, ks_stat=ks_stat, ks_p=ks_p, ad_stat=ad_stat))

    plt.figure(figsize=(9, 6))
    plt.plot(xi_us, xi_vals, marker='o')
    plt.xlabel("Threshold u (s)"); plt.ylabel("Shape ξ"); plt.title("Shape (ξ) stability")
    plt.tight_layout(); plt.savefig(os.path.join(out_dir, "xi_stability.png"), dpi=150); plt.close()

    df = pd.DataFrame(rows).sort_values("u")
    df.to_csv(os.path.join(out_dir, "gpd_threshold_summary.csv"), index=False)

    manifest = {
        "out_dir": out_dir,
        "generated": datetime.utcnow().isoformat() + "Z",
        "files": {
            "mrl_curve": "mrl_curve.png",
            "xi_stability": "xi_stability.png",
            "summary_csv": "gpd_threshold_summary.csv",
            "qq_plots": sorted([f for f in os.listdir(out_dir) if f.startswith("qq_gpd_u_")])
        }
    }
    with open(os.path.join(out_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return df

def run_from_roots(
    roots: Sequence[str],
    thresholds_s: List[float],
    pattern: str = "*.csv",
    recursive: bool = True,
    max_files_per_root: Optional[int] = None,
    warmup_frac: float = 0.0,
    keep_warmup: bool = False,
    out_dir: Optional[str] = None,
    per_root: bool = True,
    max_points_per_u: Optional[int] = 5000
):
    if out_dir is None:
        out_dir = os.path.join("evt_threshold_files", f"evt_diag_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(out_dir, exist_ok=True)

    # Combined
    lat_all = load_latencies_multi(roots, pattern=pattern, recursive=recursive,
                                   max_files_per_root=max_files_per_root,
                                   warmup_frac=warmup_frac, keep_warmup=keep_warmup)
    df_all = run_threshold_diagnostics(lat_all, thresholds_s, out_dir=os.path.join(out_dir, "combined"),
                                       max_points_per_u=max_points_per_u)

    per_root_summaries = []
    if per_root:
        for r in roots:
            try:
                lat_r = load_latencies_multi([r], pattern=pattern, recursive=recursive,
                                             max_files_per_root=max_files_per_root,
                                             warmup_frac=warmup_frac, keep_warmup=keep_warmup)
                df_r = run_threshold_diagnostics(lat_r, thresholds_s,
                                                 out_dir=os.path.join(out_dir, f"per_root_{os.path.basename(os.path.normpath(r))}"),
                                                 max_points_per_u=max_points_per_u)
                df_r.insert(0, "root", r)
                per_root_summaries.append(df_r)
            except Exception as e:
                print(f"[WARN] Skipped root {r}: {e}")

    summary_path = os.path.join(out_dir, "summary_combined.csv")
    df_all.to_csv(summary_path, index=False)
    if per_root_summaries:
        all_roots_df = pd.concat(per_root_summaries, ignore_index=True)
        all_roots_df.to_csv(os.path.join(out_dir, "summary_per_root.csv"), index=False)
    manifest = {
        "out_dir": out_dir,
        "roots": list(roots),
        "generated": datetime.utcnow().isoformat() + "Z",
        "combined_summary": "summary_combined.csv",
        "per_root_summary": "summary_per_root.csv" if per_root_summaries else None
    }
    with open(os.path.join(out_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return {"out_dir": out_dir, "combined": summary_path}

if __name__ == "__main__":
    # Example: set your 5 roots here
    ROOTS = [
        "../results/20250922_121352/merge/merge-sts-0_merge-metrics/2025-09-22/", # 20 ms
    "../results/20250922_065826/merge/merge-sts-0_merge-metrics/2025-09-22/", # 3 ms
    "../results/20250922_021233/merge/merge-sts-0_merge-metrics/2025-09-22/", # 5 ms
    "../results/20250922_163656/merge/merge-sts-0_merge-metrics/2025-09-22/", # 15 ms
    "../results/20250923_132141/merge/merge-sts-0_merge-metrics/2025-09-22/", # 10 ms
    ]
    THRESHOLDS_S = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    PATTERN = "*.csv"    # or "*.csv"
    RECURSIVE = True
    MAX_FILES_PER_DIR = None # e.g., 200
    WARMUP_FRAC = 0.0        # e.g., 0.02
    KEEP_WARMUP = True
    OUT_DIR = None           # auto timestamp if None

    try:
        info = run_from_roots(
            ROOTS, THRESHOLDS_S,
            pattern=PATTERN, recursive=RECURSIVE,
            max_files_per_root=MAX_FILES_PER_DIR,
            warmup_frac=WARMUP_FRAC, keep_warmup=KEEP_WARMUP,
            out_dir=OUT_DIR, per_root=True, max_points_per_u=5000
        )
        print("Diagnostics saved under:", info["out_dir"])
    except Exception as e:
        print("ERROR:", e)
