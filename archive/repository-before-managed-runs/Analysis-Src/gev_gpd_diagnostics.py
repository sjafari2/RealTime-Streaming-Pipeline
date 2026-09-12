# -*- coding: utf-8 -*-
"""
gev_gpd_diagnostics.py

Diagnostics helpers for GEV/GPD fits, designed to work with the 'experiments' list
returned by latency_evt_workflow.analyze_experiment().
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

# -------- Classification helpers --------

def classify_gev_type(c: float, tol: float = 0.05) -> str:
    """Map shape parameter c to a named GEV family."""
    if np.isnan(c):
        return "unknown"
    if c > tol:
        return "Fréchet (heavy-tailed)"
    if c < -tol:
        return "Weibull (upper-bounded)"
    return "Gumbel (light/exponential tail)"

def summarize_from_summary_csv(path: str) -> pd.DataFrame:
    """Read the gev_gpd_summary.csv and add type columns."""
    df = pd.read_csv(path)
    df['gev_type'] = df['gev_c'].apply(lambda x: classify_gev_type(float(x) if pd.notna(x) else np.nan))
    # GPD type mirrors GEV sign conventions for tail heaviness
    df['gpd_type_hint'] = df['gpd_c'].apply(lambda x: "heavy (xi>0)" if pd.notna(x) and float(x) > 0 else ("bounded (xi<0)" if pd.notna(x) and float(x) < 0 else ("exponential-like (xi≈0)" if pd.notna(x) else "n/a")))
    return df

# -------- Improved plots for block maxima --------

def plot_block_maxima_zoomed(exps, out_png: str, pmax: float = 95.0, show: bool = True):
    """Zoom on the body of the distribution up to the pmax percentile."""
    fig, ax = plt.subplots()
    # determine global cutoff
    vmaxs = []
    for e in exps:
        vals = e['maxima']
        if vals.size:
            vmaxs.append(np.nanpercentile(vals, pmax))
    if not vmaxs:
        return
    cutoff = float(np.nanmax(vmaxs))
    for e in exps:
        vals = e['maxima']
        if vals.size == 0:
            continue
        ax.hist(vals[vals <= cutoff], bins=60, density=True, histtype="step", label=f"{e['label']} hist<=p{int(pmax)}")
    ax.set_xlabel("Block Maxima (s)")
    ax.set_ylabel("Density")
    ax.set_title(f"Block Maxima (<= p{int(pmax)})")
    if ax.get_legend_handles_labels()[0]:
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show: plt.show()
    plt.close(fig)

def plot_block_maxima_tail_only(exps, out_png: str, pmin: float = 95.0, show: bool = True):
    """Tail-only histogram, starting at pmin percentile, with log-y."""
    fig, ax = plt.subplots()
    vmins = []
    for e in exps:
        vals = e['maxima']
        if vals.size:
            vmins.append(np.nanpercentile(vals, pmin))
    if not vmins:
        return
    start = float(np.nanmin(vmins))
    for e in exps:
        vals = e['maxima']
        if vals.size == 0:
            continue
        tail = vals[vals >= start]
        if tail.size == 0:
            continue
        ax.hist(tail, bins=60, density=True, histtype="step", label=f"{e['label']} tail>=p{int(pmin)}")
    ax.set_yscale("log")
    ax.set_xlabel("Block Maxima (s)")
    ax.set_ylabel("Density (log)")
    ax.set_title(f"Block Maxima Tail (>= p{int(pmin)})")
    if ax.get_legend_handles_labels()[0]:
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show: plt.show()
    plt.close(fig)

# -------- Goodness-of-fit diagnostics --------

def ks_test_gev(values: np.ndarray, params: dict) -> dict:
    """One-sample KS test for GEV with given params on raw maxima."""
    vals = np.asarray(values, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size < 5 or params is None:
        return dict(stat=np.nan, pvalue=np.nan, n=len(vals))
    c, loc, scale = params['c'], params['loc'], params['scale']
    cdf = lambda x: stats.genextreme.cdf(x, c=c, loc=loc, scale=scale)
    stat, p = stats.kstest(vals, cdf)
    return dict(stat=float(stat), pvalue=float(p), n=int(vals.size))

def ks_test_gpd(exceedances: np.ndarray, params: dict) -> dict:
    """One-sample KS test for GPD on exceedances (already thresholded)."""
    exc = np.asarray(exceedances, dtype=float)
    exc = exc[np.isfinite(exc)]
    if exc.size < 10 or params is None:
        return dict(stat=np.nan, pvalue=np.nan, n=len(exc))
    cdf = lambda x: stats.genpareto.cdf(x, c=params['c'], loc=params['loc'], scale=params['scale'])
    stat, p = stats.kstest(exc, cdf)
    return dict(stat=float(stat), pvalue=float(p), n=int(exc.size))

def qqplot(values: np.ndarray, dist, params: dict, out_png: str, title: str, show: bool = True):
    """Generic QQ-plot for a given scipy dist and fitted params."""
    vals = np.asarray(values, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size < 5 or params is None:
        return
    # empirical quantiles
    qs = np.linspace(0.01, 0.99, 99)  # avoid extremes
    emp_q = np.quantile(vals, qs)
    # theoretical quantiles
    if dist == 'gev':
        th_q = stats.genextreme.ppf(qs, c=params['c'], loc=params['loc'], scale=params['scale'])
    elif dist == 'gpd':
        th_q = stats.genpareto.ppf(qs, c=params['c'], loc=params['loc'], scale=params['scale'])
    else:
        return
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.plot(th_q, emp_q, '.', label="Empirical vs Theoretical")
    # 45-degree line
    low = min(th_q.min(), emp_q.min())
    high = max(th_q.max(), emp_q.max())
    ax.plot([low, high], [low, high], '-', label="y=x")
    ax.set_xlabel("Theoretical quantiles")
    ax.set_ylabel("Empirical quantiles")
    ax.set_title(title)
    if ax.get_legend_handles_labels()[0]:
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show: plt.show()
    plt.close(fig)

def plot_block_maxima_body_no_outliers(exps, out_png: str, threshold_s: float = 1.0, refit: bool = False, show: bool = True):
    """
    Plot the body of the block-maxima distribution with outliers removed.
    Uses values <= threshold_s and overlays (optionally) a GEV PDF.

    If refit=True, the GEV is re-fitted on the truncated body for each experiment.
    Otherwise, the original GEV params are used only for visual reference (note: they
    are fitted on full data and won't match the truncated body perfectly).
    """
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy import stats

    # Build a global x-grid from the union of (min, threshold_s) across exps
    mins = []
    for e in exps:
        vals = e['maxima']
        if vals.size:
            mins.append(np.nanmin(vals))
    if not mins:
        return
    x_lo = float(np.nanmin(mins))
    x_hi = float(threshold_s)
    if not np.isfinite(x_lo) or not np.isfinite(x_hi) or x_hi <= x_lo:
        x_lo, x_hi = 0.0, float(threshold_s)

    fig, ax = plt.subplots()
    for e in exps:
        vals = e['maxima']
        if vals.size == 0:
            continue
        body = vals[vals <= threshold_s]
        if body.size == 0:
            continue
        ax.hist(body, bins=60, density=True, histtype="step", label=f"{e['label']} body (≤ {threshold_s:.2f}s)")
    # Optional GEV overlay
    xs = np.linspace(x_lo, x_hi, 400)
    for e in exps:
        vals = e['maxima']
        if vals.size == 0:
            continue
        if refit:
            # Refit GEV on the body only (c, loc, scale)
            try:
                c, loc, scale = stats.genextreme.fit(vals[vals <= threshold_s])
                scale = max(scale, 1e-9)
                pdf = stats.genextreme.pdf(xs, c=c, loc=loc, scale=scale)
                ax.plot(xs, pdf, linestyle='--', label=f"{e['label']} GEV (refit on body)")
            except Exception:
                pass
        else:
            p = e.get('gev_params')
            if p:
                pdf = stats.genextreme.pdf(xs, c=p['c'], loc=p['loc'], scale=p['scale'])
                ax.plot(xs, pdf, linestyle='--', label=f"{e['label']} GEV (full-fit ref)")

    ax.set_xlabel("Block Maxima (s)")
    ax.set_ylabel("Density")
    ax.set_title(f"Block Maxima – body only (≤ {threshold_s:.2f}s)")
    handles, labels = ax.get_legend_handles_labels()
    if handles and any(l and not str(l).startswith('_') for l in labels):
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show:
        plt.show()
    plt.close(fig)
