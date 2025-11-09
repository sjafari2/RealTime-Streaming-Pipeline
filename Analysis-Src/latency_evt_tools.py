
""" 
latency_evt_tools.py
====================
Compact toolkit for latency block‑maxima analysis (EVT/GEV), fit diagnostics,
and simple throughput metrics directly from Parquet files.

Design goals
------------
- Small, dependency‑light: numpy, pandas, scipy, matplotlib (for optional plots).
- Pure, composable functions with clear docstrings and inline comments.
- Works on directory trees with many Parquet files; supports file limiting and
  per‑subdirectory ("consumer shard") sampling for scalable analyses.
- Optional event‑time windowing so you can focus on specific periods.

Conventions
-----------
- A "block" is one Parquet file by default (per‑file block‑maxima).
- SciPy's GEV shape parameter 'c' uses the convention c = -xi (EVT shape).
- For timestamp parsing we favor an explicit `time_col`; otherwise we try to detect.
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import math
import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import genextreme as gev


# matplotlib is only needed for the optional plot helpers
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Internal helpers: file limiting/sampling
# ---------------------------------------------------------------------------
def _limit_files(
    files: List[Path],
    max_files: Optional[int] = None,
    sample: str = "head",
    seed: Optional[int] = None,
) -> List[Path]:
    """
    Down‑select a sorted list of files deterministically.

    Parameters
    ----------
    files : list[Path]
        Sorted list of file paths.
    max_files : int or None
        Keep at most this many; None or <=0 means "no limit".
    sample : {"head","tail","random","even"}
        How to choose the subset:
          - "head": first N after sorting
          - "tail": last N after sorting
          - "random": random N without replacement (use `seed` for reproducibility)
          - "even": evenly spaced across the list
    seed : int or None
        RNG seed used when `sample="random"`.

    Returns
    -------
    list[Path]
        Selected files in ascending order of path.
    """
    if max_files is None or max_files <= 0 or len(files) <= max_files:
        return files

    n = max_files
    if sample == "head":
        return files[:n]
    if sample == "tail":
        return files[-n:]

    import numpy as _np

    if sample == "random":
        rng = _np.random.default_rng(seed)
        idx = rng.choice(len(files), size=n, replace=False)
        return [files[i] for i in sorted(idx)]
    if sample == "even":
        idx = _np.linspace(0, len(files) - 1, num=n, dtype=int)
        return [files[i] for i in idx]

    # Fallback: behave like "head"
    return files[:n]


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def find_parquet_files(
    root: Path | str,
    patterns: Sequence[str] = ("*.parquet", "*.pq"),
    max_files: Optional[int] = None,
    sample: str = "head",
    seed: Optional[int] = None,
) -> List[Path]:
    """
    Recursively list Parquet files under `root` and optionally limit/sample them.

    Parameters
    ----------
    root : str | Path
        Root directory to search recursively.
    patterns : sequence of str
        Glob patterns to match (default: "*.parquet", "*.pq").
    max_files, sample, seed :
        See `_limit_files()` for semantics.

    Returns
    -------
    list[Path]
        Sorted (deterministic) list of matching files, possibly down‑selected.
    """
    root = Path(root)
    files: List[Path] = []
    for pat in patterns:
        files.extend(root.rglob(pat))
    files = sorted(set(files))  # de‑dupe, deterministic order
    return _limit_files(files, max_files=max_files, sample=sample, seed=seed)


# ---------------------------------------------------------------------------
# Column detection helpers
# ---------------------------------------------------------------------------
def detect_latency_column(
    df: pd.DataFrame,
    preferred: Sequence[str] = (
        "e2e_latency_ms",
        "latency_ms",
        "end_to_end_latency_ms",
        "consumer_latency_ms",
        "app_latency_ms",
    ),
) -> Optional[str]:
    """
    Choose a latency column from a DataFrame.

    Strategy
    --------
    1) Exact match against `preferred` names (in order).
    2) Otherwise the first column containing "latency" (case‑insensitive).

    Returns
    -------
    str or None
        Selected column name, or None if no suitable column is found.
    """
    cols_lower = {c.lower(): c for c in df.columns}
    for name in preferred:
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    for c in df.columns:
        if "latency" in c.lower():
            return c
    return None


def _detect_time_column(
    df: pd.DataFrame,
    preferred: Sequence[str] = (
        "app_done_ts",
        "consumer_recv_ts",
        "producer_send_ts",
        "timestamp",
        "time",
    ),
) -> Optional[str]:
    """
    Choose a timestamp column for event‑time operations.

    Strategy
    --------
    1) Exact match against `preferred` names (in order).
    2) Otherwise the first column that looks like time: contains "time",
       "timestamp", or ends with "ts" (case‑insensitive).
    """
    cols_lower = {c.lower(): c for c in df.columns}
    for name in preferred:
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    for c in df.columns:
        cl = c.lower()
        if "time" in cl or cl.endswith("ts") or "timestamp" in cl:
            return c
    return None


def _detect_size_column(
    df: pd.DataFrame,
    preferred: Sequence[str] = (
        "size_bytes",
        "message_size_bytes",
        "payload_bytes",
        "bytes",
        "size",
    ),
) -> Optional[str]:
    """
    Choose a size/bytes column for throughput.

    Strategy
    --------
    1) Exact match against `preferred` names (in order).
    2) Otherwise the first column containing "byte" or "size" (case‑insensitive).
    """
    cols_lower = {c.lower(): c for c in df.columns}
    for name in preferred:
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    for c in df.columns:
        cl = c.lower()
        if "byte" in cl or "size" in cl:
            return c
    return None


# ---------------------------------------------------------------------------
# Read latency series (with optional event‑time window)
# ---------------------------------------------------------------------------
def read_latency_series(
    parquet_path: Path | str,
    col: Optional[str] = None,
    preferred: Sequence[str] = (
        "e2e_latency_ms",
        "latency_ms",
        "end_to_end_latency_ms",
        "consumer_latency_ms",
        "app_latency_ms",
    ),
    time_col: Optional[str] = None,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
) -> Optional[pd.Series]:
    """
    Load a Parquet file and return a numeric latency Series, optionally filtered
    to an event‑time window.

    Parameters
    ----------
    parquet_path : str | Path
        Input Parquet file.
    col : str or None
        Explicit latency column; if None, try `preferred` or "latency" heuristic.
    preferred : sequence of str
        Candidate latency names in priority order.
    time_col : str or None
        Event‑time column to apply [time_start, time_end) filter. If None, try to detect.
    time_start, time_end : str or None
        Window boundaries (inclusive start, exclusive end). Any pandas‑parsable datetime.

    Returns
    -------
    pandas.Series or None
        Numeric latency series with NaNs dropped, or None if not found/empty.
    """
    try:
        df = pd.read_parquet(parquet_path)
    except Exception:
        return None

    # Choose latency column
    sel = col if (col and col in df.columns) else detect_latency_column(df, preferred)
    if not sel:
        return None

    # Optional event‑time filter
    if time_start or time_end:
        tcol = time_col if (time_col and time_col in df.columns) else _detect_time_column(df)
        if tcol and tcol in df.columns:
            tseries = pd.to_datetime(df[tcol], errors="coerce")
            mask = pd.Series(True, index=df.index)
            if time_start:
                mask &= tseries >= pd.to_datetime(time_start)
            if time_end:
                mask &= tseries < pd.to_datetime(time_end)
            df = df.loc[mask]

    if df.empty:
        return None

    s = pd.to_numeric(df[sel], errors="coerce").dropna()
    if s.empty:
        return None
    return s


# ---------------------------------------------------------------------------
# Block maxima collection
# ---------------------------------------------------------------------------
def block_max_of_parquet(parquet_path: Path | str, col: Optional[str] = None) -> Optional[float]:
    """
    Compute the maximum latency value in one Parquet file.

    Returns
    -------
    float or None
        Block maximum, or None if the file/column cannot be read.
    """
    s = read_latency_series(parquet_path, col=col)
    return None if s is None else float(np.max(s.values))


def _relative_group_key(root: Path, f: Path, group_level: int = 1) -> str:
    """
    Group key builder for per‑subdirectory ("consumer shard") sampling.

    Example
    -------
    If root=/data and file is /data/podA/day=2025-09-17/part-*.parquet
    - group_level=1  -> "podA"
    - group_level=2  -> "podA/day=2025-09-17"
    """
    rel = f.relative_to(root)
    parts = list(rel.parts[:-1])  # drop filename
    key_parts = parts[:group_level]
    return "/".join(key_parts) if key_parts else ""


def select_files_per_subdir(
    root: Path | str,
    patterns: Sequence[str] = ("*.parquet", "*.pq"),
    per_dir: int = 1,
    group_level: int = 1,
    sample: str = "head",
    seed: Optional[int] = None,
) -> List[Path]:
    """
    Select up to `per_dir` files from each subdirectory group ("shard").

    Parameters
    ----------
    root : str | Path
        Root directory.
    patterns : sequence of str
        Parquet filename patterns.
    per_dir : int
        Number of files to select per shard.
    group_level : int
        How many directory levels under `root` form the shard key.
    sample : {"head","tail","random","even"}
        Within each shard, how to choose the files.
    seed : int or None
        RNG seed for "random".
    """
    root = Path(root)
    files = find_parquet_files(root, patterns=patterns)

    # Group files by shard key
    groups: Dict[str, List[Path]] = {}
    for f in files:
        key = _relative_group_key(root, f, group_level=group_level)
        groups.setdefault(key, []).append(f)

    # Select per group
    selected: List[Path] = []
    for _key, flist in groups.items():
        flist = sorted(flist)
        chosen = _limit_files(flist, max_files=per_dir, sample=sample, seed=seed)
        selected.extend(chosen)

    return sorted(selected)


def collect_block_maxima(
    root: Path | str,
    col: Optional[str] = None,
    patterns: Sequence[str] = ("*.parquet", "*.pq"),
    max_files: Optional[int] = None,
    sample: str = "head",
    seed: Optional[int] = None,
    per_dir: Optional[int] = None,
    group_level: int = 1,
    time_col: Optional[str] = None,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
) -> np.ndarray:
    """
    Walk a directory tree and compute per‑file latency maxima (block maxima).

    Selection
    ---------
    - Use `per_dir` + `group_level` to pick N files per subdirectory (shard), OR
    - Use `max_files` + `sample` + `seed` for global limiting/sampling.

    Event‑time window
    -----------------
    If `time_start`/`time_end` are provided, rows within each Parquet are filtered
    by an event time column (`time_col` or detected).

    Returns
    -------
    np.ndarray
        Clean array of block maxima (NaN/inf dropped).
    """
    # Choose which files to read
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(
            root, patterns=patterns, per_dir=per_dir, group_level=group_level, sample=sample, seed=seed
        )
    else:
        files = find_parquet_files(root, patterns=patterns, max_files=max_files, sample=sample, seed=seed)

    maxima: List[float] = []
    for f in files:
        s = read_latency_series(f, col=col, time_col=time_col, time_start=time_start, time_end=time_end)
        m = None if s is None else float(np.max(s.values))
        if m is not None and np.isfinite(m):
            maxima.append(float(m))

    arr = np.array(maxima, dtype=float)
    return arr[np.isfinite(arr)]


# ---------------------------------------------------------------------------
# GEV fit + helpers
# ---------------------------------------------------------------------------
def fit_gev(block_maxima: np.ndarray) -> Tuple[float, float, float]:
    """
    Fit a GEV distribution to block maxima via MLE (SciPy).

    Parameters
    ----------
    block_maxima : np.ndarray
        Array of per‑block maxima.

    Returns
    -------
    (c, loc, scale) : tuple of float
        SciPy's GEV params; remember EVT shape xi = -c.

    Notes
    -----
    For stability, prefer at least ~30 blocks; enforce >=10 here.
    """
    if block_maxima is None or len(block_maxima) < 10:
        raise ValueError("Not enough blocks for a stable GEV fit (need >= 10, preferably >= 30).")
    c, loc, scale = gev.fit(block_maxima)
    return float(c), float(loc), float(scale)


def gev_params_dict(c: float, loc: float, scale: float) -> Dict[str, float]:
    """
    Convenience wrapper that returns both SciPy 'c' and EVT 'xi' along with loc/scale.
    """
    xi = -float(c)
    return {"shape_c": float(c), "xi": xi, "loc": float(loc), "scale": float(scale)}


def gev_quantiles(probs: Sequence[float], c: float, loc: float, scale: float) -> Dict[float, float]:
    """
    Compute GEV quantiles for given probabilities.
    """
    return {float(p): float(gev.ppf(p, c, loc=loc, scale=scale)) for p in probs}


def return_level(period_blocks: float, c: float, loc: float, scale: float) -> float:
    """
    Compute the T‑block return level (exceeded on average once every T blocks).

    Parameters
    ----------
    period_blocks : float
        Return period in number of blocks (files). For per‑minute blocks,
        period_blocks=60 approximates the 1‑hour return level.
    """
    if period_blocks <= 1:
        raise ValueError("Return period must be > 1 block.")
    p = 1.0 - 1.0 / float(period_blocks)
    return float(gev.ppf(p, c, loc=loc, scale=scale))


def summarize_block_maxima(block_maxima: np.ndarray, percentiles: Sequence[float] = (50, 90, 95, 99)) -> Dict[str, float]:
    """
    Quick descriptive stats for block maxima.
    """
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    if bm.size == 0:
        raise ValueError("Empty block maxima array.")
    out = {
        "count": int(bm.size),
        "mean": float(np.mean(bm)),
        "std": float(np.std(bm, ddof=1)) if bm.size > 1 else 0.0,
        "min": float(np.min(bm)),
        "max": float(np.max(bm)),
    }
    for p in percentiles:
        out[f"p{int(p)}"] = float(np.percentile(bm, p))
    return out


def save_block_maxima_csv(block_maxima: np.ndarray, out_path: Path | str) -> Path:
    """
    Save block maxima to CSV (single column 'block_max').
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"block_max": np.asarray(block_maxima, dtype=float)}).to_csv(out_path, index=False)
    return out_path


# ---------------------------------------------------------------------------
# One‑shot pipeline (directory -> maxima -> GEV -> quantiles)
# ---------------------------------------------------------------------------
def analyze_parquet_dir(
    root: Path | str,
    col: Optional[str] = None,
    probs: Sequence[float] = (0.99, 0.999),
    save_dir: Optional[Path | str] = None,
    patterns: Sequence[str] = ("*.parquet", "*.pq"),
    max_files: Optional[int] = None,
    sample: str = "head",
    seed: Optional[int] = None,
    per_dir: Optional[int] = None,
    group_level: int = 1,
    time_col: Optional[str] = None,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
) -> Dict[str, object]:
    """
    End‑to‑end helper: find files → per‑file maxima → GEV fit → quantiles.
    Supports the same file limiting/per‑dir options and event‑time windowing.

    If `save_dir` is provided, writes:
    - block_maxima.csv
    - gev_summary.json (params, quantiles, counts, selection details)
    """
    root = Path(root)
    all_files = find_parquet_files(root, patterns=patterns)

    if per_dir is not None and per_dir > 0:
        used_files = select_files_per_subdir(root, patterns=patterns, per_dir=per_dir, group_level=group_level, sample=sample, seed=seed)
    else:
        used_files = find_parquet_files(root, patterns=patterns, max_files=max_files, sample=sample, seed=seed)

    bm = collect_block_maxima(
        root,
        col=col,
        patterns=patterns,
        max_files=max_files,
        sample=sample,
        seed=seed,
        per_dir=per_dir,
        group_level=group_level,
        time_col=time_col,
        time_start=time_start,
        time_end=time_end,
    )

    c, loc, scale = fit_gev(bm)
    params = gev_params_dict(c, loc, scale)
    quants = gev_quantiles(probs, c, loc, scale)

    saved: Dict[str, str] = {}
    if save_dir is not None:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        bm_csv = save_dir / "block_maxima.csv"
        save_block_maxima_csv(bm, bm_csv)
        saved["block_maxima_csv"] = str(bm_csv)

        import json

        meta = {
            "root": str(root),
            "files_found": len(all_files),
            "files_used": len(used_files),
            "params": params,
            "quantiles": quants,
            "sample": sample,
            "max_files": max_files,
            "per_dir": per_dir,
            "group_level": group_level,
            "seed": seed,
            "time_start": time_start,
            "time_end": time_end,
        }
        meta_json = save_dir / "gev_summary.json"
        meta_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        saved["gev_summary_json"] = str(meta_json)

    return {
        "root": str(root),
        "files_found": len(all_files),
        "files_used": len(used_files),
        "block_count": int(bm.size),
        "block_maxima": bm,
        "params": params,
        "quantiles": quants,
        "saved": saved,
        "sample": sample,
        "max_files": max_files,
        "per_dir": per_dir,
        "group_level": group_level,
        "seed": seed,
        "time_start": time_start,
        "time_end": time_end,
    }


# ---------------------------------------------------------------------------
# GEV diagnostics (goodness of fit and plots)
# ---------------------------------------------------------------------------
def gev_ks_test(block_maxima: np.ndarray, c: float, loc: float, scale: float) -> Dict[str, float]:
    """
    Kolmogorov–Smirnov test: empirical CDF of block maxima vs fitted GEV CDF.

    Returns
    -------
    dict
        {"stat": D, "pvalue": p}
    """
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    if bm.size < 10:
        raise ValueError("Need at least 10 points for KS test.")
    D, p = stats.kstest(bm, lambda x: gev.cdf(x, c, loc=loc, scale=scale))
    return {"stat": float(D), "pvalue": float(p)}


def qq_data_gev(block_maxima: np.ndarray, c: float, loc: float, scale: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    Prepare data for a Q–Q plot (theoretical vs empirical quantiles).
    """
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    n = bm.size
    p = (np.arange(1, n + 1) - 0.5) / n
    emp = np.sort(bm)
    theo = gev.ppf(p, c, loc=loc, scale=scale)
    return theo, emp


def pp_data_gev(block_maxima: np.ndarray, c: float, loc: float, scale: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    Prepare data for a P–P plot (model CDF vs empirical CDF). Returns (model_p, emp_p).
    """
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    n = bm.size
    emp = np.sort(bm)
    emp_p = (np.arange(1, n + 1)) / (n + 1.0)
    model_p = gev.cdf(emp, c, loc=loc, scale=scale)
    return model_p, emp_p


def plot_hist_with_gev(block_maxima: np.ndarray, c: float, loc: float, scale: float, bins: int = 50, density: bool = True):
    """
    Plot histogram of block maxima with fitted GEV PDF overlay.
    (Uses matplotlib; no colors/styles are explicitly set.)
    """
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    fig = plt.figure()
    ax = fig.gca()
    ax.hist(bm, bins=bins, density=density)
    xs = np.linspace(np.min(bm), np.max(bm), 400)
    ax.plot(xs, gev.pdf(xs, c, loc=loc, scale=scale))
    ax.set_xlabel("Block maxima (same units as data)")
    ax.set_ylabel("Density" if density else "Count")
    ax.set_title("Histogram of block maxima with fitted GEV PDF")
    plt.show()


def plot_qq_gev(block_maxima: np.ndarray, c: float, loc: float, scale: float):
    """
    Q–Q plot (empirical vs theoretical quantiles) for a fitted GEV.
    """
    theo, emp = qq_data_gev(block_maxima, c, loc, scale)
    fig = plt.figure()
    ax = fig.gca()
    ax.scatter(theo, emp, s=10)
    lim_min = min(np.min(theo), np.min(emp))
    lim_max = max(np.max(theo), np.max(emp))
    ax.plot([lim_min, lim_max], [lim_min, lim_max])
    ax.set_xlabel("Theoretical quantiles (GEV)")
    ax.set_ylabel("Empirical quantiles")
    ax.set_title("GEV Q–Q plot")
    plt.show()


def plot_pp_gev(block_maxima: np.ndarray, c: float, loc: float, scale: float):
    """
    P–P plot (empirical probabilities vs model CDF) for a fitted GEV.
    """
    model_p, emp_p = pp_data_gev(block_maxima, c, loc, scale)
    fig = plt.figure()
    ax = fig.gca()
    ax.scatter(model_p, emp_p, s=10)
    ax.plot([0, 1], [0, 1])
    ax.set_xlabel("Model CDF (GEV)")
    ax.set_ylabel("Empirical CDF")
    ax.set_title("GEV P–P plot")
    plt.show()


# ---------------------------------------------------------------------------
# Throughput from Parquet (time‑series msgs/bytes/MBps)
# ---------------------------------------------------------------------------
def _read_time_size(
    parquet_path: Path | str,
    time_col: Optional[str] = None,
    size_col: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Read one Parquet and return a tiny DataFrame with columns ['time','bytes'].

    - Parses time to pandas datetime.
    - Coerces size to numeric (optional; may be NaN if no size column is present).
    """
    try:
        df = pd.read_parquet(parquet_path)
    except Exception:
        return None

    tcol = time_col if (time_col and time_col in df.columns) else _detect_time_column(df)
    if not tcol:
        return None

    scol = size_col if (size_col and size_col in df.columns) else _detect_size_column(df)

    tmp = pd.DataFrame({"time": pd.to_datetime(df[tcol], errors="coerce")})
    if scol and scol in df.columns:
        tmp["bytes"] = pd.to_numeric(df[scol], errors="coerce")
    else:
        tmp["bytes"] = np.nan

    tmp = tmp.dropna(subset=["time"])
    return tmp if not tmp.empty else None


def throughput_from_parquet_dir(
    root: Path | str,
    time_col: Optional[str] = None,
    size_col: Optional[str] = None,
    resample: str = "1S",
    # File limiting & grouping
    max_files: Optional[int] = None,
    sample: str = "head",
    seed: Optional[int] = None,
    per_dir: Optional[int] = None,
    group_level: int = 1,
    # Event‑time window
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
    # Optional console progress
    show_progress: bool = False,
    progress_every: int = 500,
) -> pd.DataFrame:
    """
    Build a time‑series throughput table: msgs per window, bytes per window, MB/s.

    Selection/windowing matches the GEV helpers:
    - Limit files via `max_files`/`sample`/`seed` or pick N per shard via `per_dir`/`group_level`.
    - Filter rows inside each Parquet using `[time_start, time_end)` if provided.

    Notes
    -----
    If timestamps have low granularity (e.g., all identical), resampling can
    collapse into one bin producing unrealistically large values.
    Consider inspecting with `debug_throughput_scan` and adjusting `resample`.
    """
    # Choose which files to scan
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=seed)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=seed)

    frames = []
    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f)
        except Exception:
            continue

        # pick time column
        tcol = time_col if (time_col and time_col in df.columns) else _detect_time_column(df)
        if not tcol:
            continue

        # pick size column (optional)
        scol = size_col if (size_col and size_col in df.columns) else _detect_size_column(df)

        tmp = pd.DataFrame({"time": pd.to_datetime(df[tcol], errors="coerce")})
        if time_start or time_end:
            mask = pd.Series(True, index=tmp.index)
            if time_start:
                mask &= tmp["time"] >= pd.to_datetime(time_start)
            if time_end:
                mask &= tmp["time"] < pd.to_datetime(time_end)
            tmp = tmp.loc[mask]

        if scol and scol in df.columns:
            tmp["bytes"] = pd.to_numeric(df[scol], errors="coerce")
        else:
            tmp["bytes"] = np.nan

        tmp = tmp.dropna(subset=["time"])
        if not tmp.empty:
            frames.append(tmp)

        if show_progress and (i % max(1, progress_every) == 0):
            print(f"[throughput] processed {i}/{len(files)} files")

    if not frames:
        raise ValueError("No usable time/size data found in Parquet files. Check time_col/size_col and window.")

    df_all = pd.concat(frames, ignore_index=True).sort_values("time").set_index("time")

    # msgs per window: count rows in each bin
    msgs = df_all["bytes"].groupby(pd.Grouper(freq=resample)).size().rename("msgs_per_window")

    # bytes per window (if any bytes present)
    if df_all["bytes"].notna().any():
        bytes_sum = df_all["bytes"].groupby(pd.Grouper(freq=resample)).sum(min_count=1).rename("bytes_per_window")
        out = pd.concat([msgs, bytes_sum], axis=1)
        sec = pd.to_timedelta(resample).total_seconds() or 1.0
        out["MB_per_sec"] = out["bytes_per_window"] / (1024.0 * 1024.0) / sec
    else:
        out = msgs.to_frame()
        out["bytes_per_window"] = np.nan
        out["MB_per_sec"] = np.nan

    return out


def summarize_throughput(ts_df: pd.DataFrame, percentiles: Sequence[float] = (50, 90, 95, 99)) -> Dict[str, float]:
    """
    Summarize a throughput time series (msgs and MB/s). Returns mean/median and selected percentiles.
    """
    out: Dict[str, float] = {}
    if "MB_per_sec" in ts_df:
        series = ts_df["MB_per_sec"].dropna()
        if not series.empty:
            out["MBps_mean"] = float(series.mean())
            out["MBps_median"] = float(series.median())
            for p in percentiles:
                out[f"MBps_p{int(p)}"] = float(np.percentile(series, p))
    if "msgs_per_window" in ts_df:
        s = ts_df["msgs_per_window"].dropna()
        if not s.empty:
            out["msgs_mean"] = float(s.mean())
            out["msgs_median"] = float(s.median())
    return out


def debug_throughput_scan(
    root: Path | str,
    time_col: Optional[str] = None,
    size_col: Optional[str] = None,
    sample: str = "head",
    max_files: Optional[int] = 200,
) -> Dict[str, object]:
    """
    Diagnose which time/size columns are used and the effective time span / granularity.

    Returns
    -------
    dict with keys:
      - files_scanned, rows_total
      - time_col_used, size_col_used
      - time_min, time_max, time_span_sec
      - unique_seconds, unique_minutes
      - example_times (first few parsed values)
    """
    files = find_parquet_files(root, max_files=max_files, sample=sample)
    rows_total = 0
    times = []
    tcol_used = None
    scol_used = None
    for f in files:
        try:
            df = pd.read_parquet(f)
        except Exception:
            continue
        tcol = time_col if (time_col and time_col in df.columns) else _detect_time_column(df)
        if not tcol:
            continue
        scol = size_col if (size_col and size_col in df.columns) else _detect_size_column(df)
        t = pd.to_datetime(df[tcol], errors="coerce")
        nn = t.notna()
        rows_total += int(nn.sum())
        times.append(t[nn])
        tcol_used = tcol
        scol_used = scol or scol_used
    if not times:
        return {"files_scanned": len(files), "rows_total": 0, "note": "No usable time column in scanned files."}
    t_all = pd.concat(times, ignore_index=True)
    out = {
        "files_scanned": len(files),
        "rows_total": int(len(t_all)),
        "time_col_used": tcol_used,
        "size_col_used": scol_used,
        "time_min": str(t_all.min()),
        "time_max": str(t_all.max()),
        "time_span_sec": float((t_all.max() - t_all.min()).total_seconds() if len(t_all) else 0.0),
        "unique_seconds": int(t_all.dt.floor("S").nunique()),
        "unique_minutes": int(t_all.dt.floor("T").nunique()),
        "example_times": [str(x) for x in t_all.head(5).tolist()],
    }
    return out


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------
def fit_gev_from_dir(
    root: Path | str,
    col: Optional[str] = None,
    probs: Sequence[float] = (0.99, 0.999),
    patterns: Sequence[str] = ("*.parquet", "*.pq"),
    # Same selectors as analyze_parquet_dir
    max_files: Optional[int] = None,
    sample: str = "head",
    seed: Optional[int] = None,
    per_dir: Optional[int] = None,
    group_level: int = 1,
    # Optional event‑time window
    time_col: Optional[str] = None,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
) -> Dict[str, object]:
    """
    Back‑compat helper: directory → per‑file maxima → GEV fit + quantiles.
    Mirrors the selection/window options of `analyze_parquet_dir`.
    """
    bm = collect_block_maxima(
        root,
        col=col,
        patterns=patterns,
        max_files=max_files,
        sample=sample,
        seed=seed,
        per_dir=per_dir,
        group_level=group_level,
        time_col=time_col,
        time_start=time_start,
        time_end=time_end,
    )
    c, loc, scale = fit_gev(bm)
    return {"block_maxima": bm, "params": gev_params_dict(c, loc, scale), "quantiles": gev_quantiles(probs, c, loc, scale)}


def simple_merge_metrics(
    root: Path | str,
    latency_col: Optional[str] = None,
    time_col: Optional[str] = None,
    size_col: Optional[str] = None,
    max_files: Optional[int] = None,
    sample: str = "head",
    gev_probs: Sequence[float] = (0.95, 0.99, 0.999),
) -> Dict[str, object]:
    """
    Minimal, straight‑to‑the‑point metrics similar to a "merge" script output.

    Parameters
    ----------
    root : str | Path
        Directory containing Parquet files.
    latency_col, time_col, size_col : str or None
        Column names (explicit is best); if None, latency/time may be auto‑detected.
    max_files : int or None
        Limit how many Parquet files to read (None/<=0 = all).
    sample : {"head","tail","random","even"}
        Sampling strategy when limiting.
    gev_probs : sequence of float
        Quantiles to compute from the fitted GEV of per‑file maxima.

    Returns
    -------
    dict
        {
          "files_used": int,
          "rows_total": int,
          "latency": {count, mean, p50, p95, p99, max},
          "time_range": {start, end, duration_sec},
          "throughput_mean": {msgs_per_sec, MB_per_sec},
          "gev": {params, quantiles, blocks} or a short note if not enough blocks
        }
    """
    files = find_parquet_files(root, max_files=max_files, sample=sample)

    # Accumulate minimal columns
    lat_parts = []
    times = []
    sizes = []

    for f in files:
        try:
            df = pd.read_parquet(f)
        except Exception:
            continue

        # latency
        lcol = latency_col if (latency_col and latency_col in df.columns) else detect_latency_column(df)
        if lcol is not None and lcol in df.columns:
            lat = pd.to_numeric(df[lcol], errors="coerce").dropna()
            if not lat.empty:
                lat_parts.append(lat)

        # time
        tcol = time_col if (time_col and time_col in df.columns) else _detect_time_column(df)
        if tcol is not None and tcol in df.columns:
            t = pd.to_datetime(df[tcol], errors="coerce").dropna()
            if not t.empty:
                times.append(t)

        # size (optional)
        if size_col and size_col in df.columns:
            s = pd.to_numeric(df[size_col], errors="coerce").dropna()
            if not s.empty:
                sizes.append(s)

    files_used = len(files)

    # Latency stats
    if lat_parts:
        lat_all = pd.concat(lat_parts, ignore_index=True)
        latency_stats = {
            "count": int(lat_all.size),
            "mean": float(lat_all.mean()),
            "p50": float(lat_all.quantile(0.50)),
            "p95": float(lat_all.quantile(0.95)),
            "p99": float(lat_all.quantile(0.99)),
            "max": float(lat_all.max()),
        }
    else:
        latency_stats = {"count": 0, "mean": np.nan, "p50": np.nan, "p95": np.nan, "p99": np.nan, "max": np.nan}

    # Time range + rough throughput
    if times:
        t_all = pd.concat(times, ignore_index=True)
        t_min = t_all.min()
        t_max = t_all.max()
        dur_sec = max(1.0, (t_max - t_min).total_seconds())
        rows_total = int(sum(len(x) for x in lat_parts)) if lat_parts else int(len(t_all))

        msgs_per_sec = rows_total / dur_sec
        if sizes:
            bytes_total = float(pd.concat(sizes, ignore_index=True).sum())
            MB_per_sec = bytes_total / (1024.0 * 1024.0) / dur_sec
        else:
            MB_per_sec = np.nan

        time_range = {"start": str(t_min), "end": str(t_max), "duration_sec": float(dur_sec)}
        throughput_mean = {"msgs_per_sec": float(msgs_per_sec), "MB_per_sec": float(MB_per_sec)}
    else:
        rows_total = int(sum(len(x) for x in lat_parts)) if lat_parts else 0
        time_range = {"start": None, "end": None, "duration_sec": np.nan}
        throughput_mean = {"msgs_per_sec": np.nan, "MB_per_sec": np.nan}

    # Block‑maxima + GEV (quick)
    gev_out: Dict[str, object] = {}
    try:
        bm = collect_block_maxima(root, col=latency_col, max_files=max_files, sample=sample)
        if bm.size >= 10:
            c, loc, scale = fit_gev(bm)
            gev_out = {
                "params": gev_params_dict(c, loc, scale),
                "quantiles": gev_quantiles(gev_probs, c, loc, scale),
                "blocks": int(bm.size),
            }
        else:
            gev_out = {"note": f"Not enough blocks for GEV (got {bm.size})."}
    except Exception as e:
        gev_out = {"error": str(e)}

    return {
        "files_used": files_used,
        "rows_total": rows_total,
        "latency": latency_stats,
        "time_range": time_range,
        "throughput_mean": throughput_mean,
        "gev": gev_out,
    }


def _to_datetime_fixed(series: pd.Series, time_unit: Optional[str] = None) -> pd.Series:
    if time_unit and pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit=time_unit, utc=True)
    return pd.to_datetime(series, errors="coerce", utc=True)

def throughput_from_parquet_dir_fixed(root: Union[Path, str],
                                      time_col: str = "consumer_receive_timestamp",
                                      size_col: str = "size_bytes",
                                      resample: str = "1S",
                                      time_unit: Optional[str] = None,
                                      max_files: Optional[int] = None,
                                      sample: str = "head",
                                      seed: Optional[int] = None,
                                      per_dir: Optional[int] = None,
                                      group_level: int = 1,
                                      show_progress: bool = False,
                                      progress_every: int = 500) -> pd.DataFrame:
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=seed)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=seed)
    frames = []
    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f, columns=[time_col, size_col])
        except Exception:
            continue
        if time_col not in df.columns:
            continue
        t = _to_datetime_fixed(df[time_col], time_unit=time_unit)
        tmp = pd.DataFrame({"time": t})
        if size_col in df.columns:
            tmp["bytes"] = pd.to_numeric(df[size_col], errors="coerce")
        else:
            tmp["bytes"] = np.nan
        tmp = tmp.dropna(subset=["time"])
        if not tmp.empty:
            frames.append(tmp)
        if show_progress and (i % max(1, progress_every) == 0):
            print(f"[throughput-fixed] {i}/{len(files)} files")
    if not frames:
        raise ValueError("No usable rows found. Check column names and time_unit.")
    df_all = pd.concat(frames, ignore_index=True).sort_values("time").set_index("time")
    msgs = df_all["bytes"].groupby(pd.Grouper(freq=resample)).size().rename("msgs_per_window")
    if df_all["bytes"].notna().any():
        bytes_sum = df_all["bytes"].groupby(pd.Grouper(freq=resample)).sum(min_count=1).rename("bytes_per_window")
        out = pd.concat([msgs, bytes_sum], axis=1)
        sec = pd.to_timedelta(resample).total_seconds() or 1.0
        out["MB_per_sec"] = out["bytes_per_window"] / (1024.0 * 1024.0) / sec
    else:
        out = msgs.to_frame()
        out["bytes_per_window"] = np.nan
        out["MB_per_sec"] = np.nan
    return out

def simple_merge_metrics_fixed(root: Union[Path, str],
                               max_files: Optional[int] = None,
                               sample: str = "head",
                               per_dir: Optional[int] = None,
                               group_level: int = 1,
                               time_col: str = "consumer_receive_timestamp",
                               latency_col: str = "end_to_end_latency_seconds",
                               size_col: str = "size_bytes",
                               time_unit: Optional[str] = None,
                               gev_probs: Sequence[float] = (0.95, 0.99, 0.999)) -> Dict[str, object]:
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=None)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=None)
    lat_parts, times, sizes = [], [], []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=[c for c in [latency_col, time_col, size_col] if c is not None])
        except Exception:
            continue
        if latency_col in df.columns:
            lat = pd.to_numeric(df[latency_col], errors="coerce").dropna()
            if not lat.empty:
                lat_parts.append(lat)
        if time_col in df.columns:
            t = _to_datetime_fixed(df[time_col], time_unit=time_unit).dropna()
            if not t.empty:
                times.append(t)
        if size_col in df.columns:
            s = pd.to_numeric(df[size_col], errors="coerce").dropna()
            if not s.empty:
                sizes.append(s)
    files_used = len(files)
    if lat_parts:
        lat_all = pd.concat(lat_parts, ignore_index=True)
        latency_stats = {"count": int(lat_all.size),
                         "mean": float(lat_all.mean()),
                         "p50": float(lat_all.quantile(0.50)),
                         "p95": float(lat_all.quantile(0.95)),
                         "p99": float(lat_all.quantile(0.99)),
                         "max": float(lat_all.max())}
    else:
        latency_stats = {"count": 0, "mean": np.nan, "p50": np.nan, "p95": np.nan, "p99": np.nan, "max": np.nan}
    if times:
        t_all = pd.concat(times, ignore_index=True)
        t_min, t_max = t_all.min(), t_all.max()
        dur_sec = max(1.0, (t_max - t_min).total_seconds())
        rows_total = int(sum(len(x) for x in lat_parts)) if lat_parts else int(len(t_all))
        msgs_per_sec = rows_total / dur_sec
        if sizes:
            bytes_total = float(pd.concat(sizes, ignore_index=True).sum())
            MB_per_sec = bytes_total / (1024.0 * 1024.0) / dur_sec
        else:
            MB_per_sec = np.nan
        time_range = {"start": str(t_min), "end": str(t_max), "duration_sec": float(dur_sec)}
        throughput_mean = {"msgs_per_sec": float(msgs_per_sec), "MB_per_sec": float(MB_per_sec)}
    else:
        rows_total = int(sum(len(x) for x in lat_parts)) if lat_parts else 0
        time_range = {"start": None, "end": None, "duration_sec": np.nan}
        throughput_mean = {"msgs_per_sec": np.nan, "MB_per_sec": np.nan}
    gev_out: Dict[str, object] = {}
    try:
        bm = collect_block_maxima(root, col=latency_col, max_files=max_files, sample=sample,
                                  per_dir=per_dir, group_level=group_level)
        if bm.size >= 10:
            c, loc, scale = fit_gev(bm)
            gev_out = {"params": gev_params_dict(c, loc, scale),
                       "quantiles": gev_quantiles(gev_probs, c, loc, scale),
                       "blocks": int(bm.size)}
        else:
            gev_out = {"note": f"Not enough blocks for GEV (got {bm.size})."}
    except Exception as e:
        gev_out = {"error": str(e)}
    return {"files_used": files_used,
            "rows_total": rows_total,
            "latency": latency_stats,
            "time_range": time_range,
            "throughput_mean": throughput_mean,
            "gev": gev_out}
