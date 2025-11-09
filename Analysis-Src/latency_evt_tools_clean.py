
# -*- coding: utf-8 -*-
"""
latency_evt_tools_clean.py
==========================
One tidy, Python 3.8+ compatible toolkit for:
  • File discovery & sampling (max_files, random/even sampling, N-per-subdir)
  • Fixed-schema throughput from Parquet (no column guessing)
  • Simple "merge-like" metrics (latency + throughput)
  • Block-maxima + GEV fit for tail latency

Assumed Parquet schema (fixed):
  - producer_timestamp
  - consumer_receive_timestamp     <-- default event time
  - application_timestamp
  - application_latency_seconds
  - end_to_end_latency_seconds     <-- default latency for EVT
  - size_bytes                     <-- used for MB/s
  - target_rate, topic, partition, offset, index (ignored here)

Note: All functions avoid PEP-604 type hints so they work on Python <3.10.
"""

import math
import json
from pathlib import Path
from typing import Optional, Sequence, Dict, List
from collections import Counter
import numpy as np
import pandas as pd
from scipy.stats import genextreme as gev
from scipy import stats

# ---------------------------------------------------------------------------
# Internal helpers: file limiting/sampling
# ---------------------------------------------------------------------------
def _limit_files(files, max_files=None, sample="head", seed=None):
    """
    Down-select a sorted list of files deterministically.

    Parameters
    ----------
    files : list[Path]
    max_files : int or None
        Keep at most this many; None or <=0 means "no limit".
    sample : {"head","tail","random","even"}
    seed : int or None
    """
    files = list(files)
    if max_files is None or max_files <= 0 or len(files) <= max_files:
        return files

    n = int(max_files)
    if sample == "head":
        return files[:n]
    if sample == "tail":
        return files[-n:]
    if sample == "random":
        rng = np.random.default_rng(seed)
        idx = rng.choice(len(files), size=n, replace=False)
        return [files[i] for i in sorted(idx)]
    if sample == "even":
        idx = np.linspace(0, len(files) - 1, num=n, dtype=int)
        return [files[i] for i in idx]
    return files[:n]  # fallback


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def find_parquet_files(root, patterns=("*.parquet", "*.pq"), max_files=None, sample="head", seed=None):
    """
    Recursively list Parquet files under `root` and optionally limit/sample them.
    Returns a sorted, de-duplicated list of Path objects.
    """
    root = Path(root)
    files = []
    for pat in patterns:
        files.extend(root.rglob(pat))
    files = sorted(set(files))
    return _limit_files(files, max_files=max_files, sample=sample, seed=seed)


def _relative_group_key(root, fpath, group_level=1):
    """Build a shard key from the relative path up to `group_level` directories."""
    rel = Path(fpath).relative_to(Path(root))
    parts = list(rel.parts[:-1])  # drop filename
    key_parts = parts[:group_level]
    return "/".join(key_parts) if key_parts else ""


def select_files_per_subdir(root, patterns=("*.parquet", "*.pq"),
                            per_dir=1, group_level=1, sample="head", seed=None):
    """
    Pick up to `per_dir` files from each subdirectory group ("shard").
    """
    root = Path(root)
    files = find_parquet_files(root, patterns=patterns)
    groups: Dict[str, List[Path]] = {}
    for f in files:
        key = _relative_group_key(root, f, group_level=group_level)
        groups.setdefault(key, []).append(f)

    selected = []
    for _, flist in groups.items():
        flist = sorted(flist)
        chosen = _limit_files(flist, max_files=per_dir, sample=sample, seed=seed)
        selected.extend(chosen)
    return sorted(selected)


# ---------------------------------------------------------------------------
# Fixed-schema datetime conversion
# ---------------------------------------------------------------------------
def _to_datetime_fixed(series, time_unit=None):
    """
    Convert a timestamp-like series to pandas datetime (UTC).
    If `time_unit` is provided and series can be coerced to numeric, use it.
    Otherwise fall back to generic parser.
    """
    # Coerce to numeric if possible
    s_num = pd.to_numeric(series, errors="coerce")
    if time_unit is not None and s_num.notna().any():
        return pd.to_datetime(s_num, unit=time_unit, utc=True)
    # Else try generic parser (works for ISO strings)
    return pd.to_datetime(series, errors="coerce", utc=True)


# ---------------------------------------------------------------------------
# Audit (fast serial; safe on 50k files if columns are narrow)
# ---------------------------------------------------------------------------
def audit_fixed(root,
                time_col="consumer_receive_timestamp",
                size_col="size_bytes",
                time_unit="s",
                max_files=None,
                sample="head",
                per_dir=None,
                group_level=1):
    """
    Scan Parquet files and return global time min/max, rows_total, bytes_total.
    Uses only `time_col` and `size_col` for speed.
    """
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=None)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=None)

    rows_total = 0
    bytes_total = 0.0
    tmin = None
    tmax = None

    for f in files:
        try:
            df = pd.read_parquet(f, columns=[c for c in [time_col, size_col] if c is not None])
        except Exception:
            continue

        if time_col in df.columns:
            t = _to_datetime_fixed(df[time_col], time_unit=time_unit)
            if t.notna().any():
                ftmin = t.min()
                ftmax = t.max()
                tmin = ftmin if tmin is None or ftmin < tmin else tmin
                tmax = ftmax if tmax is None or ftmax > tmax else tmax

        rows_total += int(len(df))
        if size_col in df.columns:
            bytes_total += float(pd.to_numeric(df[size_col], errors="coerce").fillna(0).sum())

    span_sec = float((tmax - tmin).total_seconds()) if (tmin is not None and tmax is not None) else 0.0
    MB_total = bytes_total / (1024.0 * 1024.0) if bytes_total else 0.0
    avg_msg_size_bytes = (bytes_total / rows_total) if rows_total else 0.0

    return {
        "files_scanned": len(files),
        "rows_total": rows_total,
        "tmin": tmin, "tmax": tmax, "span_sec": span_sec,
        "bytes_total": bytes_total, "MB_total": MB_total,
        "avg_msg_size_bytes": avg_msg_size_bytes,
    }


# ---------------------------------------------------------------------------
# Throughput from Parquet (fixed schema)
# ---------------------------------------------------------------------------
def throughput_from_parquet_dir_fixed(root,
                                      time_col="consumer_receive_timestamp",
                                      size_col="size_bytes",
                                      resample="1S",
                                      time_unit="s",
                                      max_files=None,
                                      sample="head",
                                      seed=None,
                                      per_dir=None,
                                      group_level=1,
                                      show_progress=False,
                                      progress_every=1000):
    """
    Build a per-window throughput series using fixed columns (no autodetect).
    Returns a DataFrame indexed by time with:
      - msgs_per_window
      - bytes_per_window
      - MB_per_sec  (bytes/window / seconds_per_window / 1,048,576)
    """
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=seed)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=seed)

    frames = []
    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f, columns=[c for c in [time_col, size_col] if c is not None])
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
            print(f"[throughput] {i}/{len(files)} files")

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


def summarize_throughput(ts_df, percentiles=(50, 90, 95, 99)):
    """
    Summarize a throughput time series. Returns mean/median and selected percentiles for MB/s,
    and mean/median for msgs/sec.
    """
    out = {}
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


# ---------------------------------------------------------------------------
# Simple "merge-like" metrics (fixed schema)
# ---------------------------------------------------------------------------
def simple_merge_metrics_fixed(root,
                               max_files=None,
                               sample="head",
                               per_dir=None,
                               group_level=1,
                               time_col="consumer_receive_timestamp",
                               latency_col="end_to_end_latency_seconds",
                               size_col="size_bytes",
                               time_unit="s",
                               gev_probs=(0.95, 0.99, 0.999)):
    """
    Minimal metrics using your exact schema.
    Returns:
      - files_used, rows_total
      - latency stats (count/mean/p50/p95/p99/max) in seconds
      - time_range (start/end/duration_sec)
      - throughput_mean (msgs_per_sec, MB_per_sec)
      - GEV of per-file maxima from `latency_col` (if enough files)
    """
    # choose files
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=None)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=None)

    lat_parts, times, sizes = [], [], []
    for f in files:
        try:
            cols = [c for c in [latency_col, time_col, size_col] if c is not None]
            df = pd.read_parquet(f, columns=cols)
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
            "min": float(lat_all.min()),
        }
    else:
        latency_stats = {"count": 0, "mean": np.nan, "p50": np.nan, "p95": np.nan, "p99": np.nan, "max": np.nan, "min": np.nan}

    # Time range + rough throughput
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

    # Block-maxima + GEV from fixed latency column
    gev_out = {}
    try:
        bm = collect_block_maxima_fixed(root, latency_col=latency_col,
                                        max_files=max_files, sample=sample,
                                        per_dir=per_dir, group_level=group_level)
        if bm.size >= 10:
            c, loc, scale = fit_gev(bm)
            gev_out = {
                "params": gev_params_dict(c, loc, scale),
                "quantiles": gev_quantiles(gev_probs, c, loc, scale),
                "blocks": int(bm.size),
            }
        else:
            gev_out = {"note": "Not enough blocks for GEV (need >= 10)."}
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


# ---------------------------------------------------------------------------
# Block maxima + GEV (fixed latency column, per-file block = one Parquet)
# ---------------------------------------------------------------------------
def collect_block_maxima_fixed(root,
                               latency_col="end_to_end_latency_seconds",
                               patterns=("*.parquet", "*.pq"),
                               max_files=None,
                               sample="head",
                               seed=None,
                               per_dir=None,
                               group_level=1):
    """
    Compute per-file maxima using a fixed latency column.
    Returns a clean numpy array of maxima.
    """
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, patterns=patterns, per_dir=per_dir, group_level=group_level, sample=sample, seed=seed)
    else:
        files = find_parquet_files(root, patterns=patterns, max_files=max_files, sample=sample, seed=seed)

    maxima = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=[latency_col])
            s = pd.to_numeric(df[latency_col], errors="coerce").dropna()
            if not s.empty:
                m = float(np.max(s.values))
                if np.isfinite(m):
                    maxima.append(m)
        except Exception:
            continue

    arr = np.asarray(maxima, dtype=float)
    return arr[np.isfinite(arr)]


def fit_gev(block_maxima):
    """Fit a GEV (SciPy) to block maxima; returns (c, loc, scale) with xi = -c."""
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    if bm.size < 10:
        raise ValueError("Not enough blocks for a stable GEV fit (need >= 10, preferably >= 30).")
    c, loc, scale = gev.fit(bm)
    return float(c), float(loc), float(scale)


def gev_params_dict(c, loc, scale):
    """Return dict with SciPy c and EVT xi along with loc/scale."""
    return {"shape_c": float(c), "xi": float(-c), "loc": float(loc), "scale": float(scale)}


def gev_quantiles(probs, c, loc, scale):
    """Compute GEV quantiles for given probabilities (iterable of floats)."""
    return {float(p): float(gev.ppf(p, c, loc=loc, scale=scale)) for p in probs}


def return_level(period_blocks, c, loc, scale):
    """T-block return level: exceeded on average once every T blocks."""
    period_blocks = float(period_blocks)
    if period_blocks <= 1.0:
        raise ValueError("Return period must be > 1 block.")
    p = 1.0 - 1.0 / period_blocks
    return float(gev.ppf(p, c, loc=loc, scale=scale))


def gev_ks_test(block_maxima, c, loc, scale):
    """Kolmogorov–Smirnov test: empirical CDF of maxima vs fitted GEV CDF."""
    bm = np.asarray(block_maxima, dtype=float)
    bm = bm[np.isfinite(bm)]
    if bm.size < 10:
        raise ValueError("Need at least 10 points for KS test.")
    D, p = stats.kstest(bm, lambda x: gev.cdf(x, c, loc=loc, scale=scale))
    return {"stat": float(D), "pvalue": float(p)}


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------
def rows_per_file_hist(root, max_files=2000, sample="head"):
    """Quick histogram: how many rows each file has (useful to explain totals)."""
    files = find_parquet_files(root, max_files=max_files, sample=sample)
    counts = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=["index"])
            counts.append(len(df))
        except Exception:
            continue
    return Counter(counts)


# ---------------------------------------------------------------------------
# Parallel audit (I/O bound; threads are fine with pyarrow/parquet)
# ---------------------------------------------------------------------------
def audit_fixed_parallel(root,
                         time_col="consumer_receive_timestamp",
                         size_col="size_bytes",
                         time_unit="s",
                         max_files=None,
                         sample="head",
                         per_dir=None,
                         group_level=1,
                         workers=8,
                         progress_every=1000):
    """
    Parallel version of `audit_fixed` using a ThreadPool to speed up scanning many files.

    Returns dict with:
      - files_scanned, rows_total
      - tmin, tmax, span_sec
      - bytes_total, MB_total
      - avg_msg_size_bytes
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Select files (same logic as serial audit)
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=None)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=None)

    if not files:
        return {"files_scanned": 0, "rows_total": 0, "note": "no parquet files found"}

    def scan_one(fpath):
        try:
            cols = [c for c in [time_col, size_col] if c is not None]
            df = pd.read_parquet(fpath, columns=cols)
        except Exception:
            return (0, None, None, 0.0)

        # rows
        r = int(len(df))

        # time min/max
        tmin = tmax = None
        if time_col in df.columns:
            t = _to_datetime_fixed(df[time_col], time_unit=time_unit)
            if t.notna().any():
                tmin = t.min()
                tmax = t.max()

        # bytes sum
        bsum = 0.0
        if size_col in df.columns:
            bsum = float(pd.to_numeric(df[size_col], errors="coerce").fillna(0).sum())

        return (r, tmin, tmax, bsum)

    rows_total = 0
    bytes_total = 0.0
    g_tmin = None
    g_tmax = None

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as ex:
        futs = [ex.submit(scan_one, f) for f in files]
        for i, fut in enumerate(as_completed(futs), 1):
            r, tmin, tmax, bsum = fut.result()
            rows_total += r
            bytes_total += bsum
            if tmin is not None:
                g_tmin = tmin if (g_tmin is None or tmin < g_tmin) else g_tmin
            if tmax is not None:
                g_tmax = tmax if (g_tmax is None or tmax > g_tmax) else g_tmax
            if progress_every and (i % progress_every == 0):
                print(f"[audit-parallel] {i}/{len(files)} files")

    span_sec = float((g_tmax - g_tmin).total_seconds()) if (g_tmin is not None and g_tmax is not None) else 0.0
    MB_total = bytes_total / (1024.0 * 1024.0) if bytes_total else 0.0
    avg_msg_size_bytes = (bytes_total / rows_total) if rows_total else 0.0

    return {
        "files_scanned": len(files),
        "rows_total": rows_total,
        "tmin": g_tmin, "tmax": g_tmax, "span_sec": span_sec,
        "bytes_total": bytes_total, "MB_total": MB_total,
        "avg_msg_size_bytes": avg_msg_size_bytes,
    }


# ---------------------------------------------------------------------------
# Multiprocess audit (useful if parquet decode is CPU-heavy)
# ---------------------------------------------------------------------------
def _audit_scan_one_mp(args):
    """
    Top-level helper for multiprocessing: (fpath, time_col, size_col, time_unit) -> tuple
    Returns: (rows_total:int, tmin:Timestamp|None, tmax:Timestamp|None, bytes_sum:float)
    """
    fpath, time_col, size_col, time_unit = args
    try:
        cols = [c for c in [time_col, size_col] if c is not None]
        df = pd.read_parquet(fpath, columns=cols)
    except Exception:
        return (0, None, None, 0.0)

    r = int(len(df))

    tmin = tmax = None
    if time_col in df.columns:
        t = _to_datetime_fixed(df[time_col], time_unit=time_unit)
        if t.notna().any():
            tmin = t.min()
            tmax = t.max()

    bsum = 0.0
    if size_col in df.columns:
        bsum = float(pd.to_numeric(df[size_col], errors="coerce").fillna(0).sum())

    return (r, tmin, tmax, bsum)


def audit_fixed_multiprocess(root,
                             time_col="consumer_receive_timestamp",
                             size_col="size_bytes",
                             time_unit="s",
                             max_files=None,
                             sample="head",
                             per_dir=None,
                             group_level=1,
                             processes=None,
                             chunksize=256,
                             progress_every=1000):
    """
    Multiprocessing version of `audit_fixed` using a process pool.

    Parameters
    ----------
    root : str | Path-like
    time_col, size_col : str
        Columns to read. Only these are loaded for speed.
    time_unit : {"s","ms","us","ns"} or None
        Unit for numeric epoch timestamps.
    max_files, sample, per_dir, group_level : selection options
    processes : int or None
        Number of worker processes (default: os.cpu_count()).
    chunksize : int
        Batch size passed to Pool.imap for better throughput.
    progress_every : int
        Print a progress line after this many files are processed.

    Returns
    -------
    dict with files_scanned, rows_total, tmin, tmax, span_sec, bytes_total, MB_total, avg_msg_size_bytes
    """
    import os
    from multiprocessing import Pool

    # Select files
    if per_dir is not None and per_dir > 0:
        files = select_files_per_subdir(root, per_dir=per_dir, group_level=group_level, sample=sample, seed=None)
    else:
        files = find_parquet_files(root, max_files=max_files, sample=sample, seed=None)

    if not files:
        return {"files_scanned": 0, "rows_total": 0, "note": "no parquet files found"}

    rows_total = 0
    bytes_total = 0.0
    g_tmin = None
    g_tmax = None

    args_iter = ((str(f), time_col, size_col, time_unit) for f in files)

    with Pool(processes=processes) as pool:
        for i, (r, tmin, tmax, bsum) in enumerate(pool.imap(_audit_scan_one_mp, args_iter, chunksize=chunksize), 1):
            rows_total += r
            bytes_total += bsum
            if tmin is not None:
                g_tmin = tmin if (g_tmin is None or tmin < g_tmin) else g_tmin
            if tmax is not None:
                g_tmax = tmax if (g_tmax is None or tmax > g_tmax) else g_tmax
            if progress_every and (i % progress_every == 0):
                print(f"[audit-mp] {i}/{len(files)} files")

    span_sec = float((g_tmax - g_tmin).total_seconds()) if (g_tmin is not None and g_tmax is not None) else 0.0
    MB_total = bytes_total / (1024.0 * 1024.0) if bytes_total else 0.0
    avg_msg_size_bytes = (bytes_total / rows_total) if rows_total else 0.0

    return {
        "files_scanned": len(files),
        "rows_total": rows_total,
        "tmin": g_tmin, "tmax": g_tmax, "span_sec": span_sec,
        "bytes_total": bytes_total, "MB_total": MB_total,
        "avg_msg_size_bytes": avg_msg_size_bytes,
    }


# ---------------------------------------------------------------------------
# FAST PATHS (big speedups, no full scans)
#   - Use Parquet footer statistics & row counts (no column reads)
#   - Use pyarrow.dataset to aggregate per-second throughput without pandas
# ---------------------------------------------------------------------------
def audit_from_metadata(root, time_col="consumer_receive_timestamp"):
    """
    Ultra-fast audit using Parquet footers only (no data read).
    Requires that the writer stored column statistics (min/max) for `time_col`.
    Returns: dict with files_scanned, rows_total, tmin, tmax, span_sec.
    """
    import pyarrow.parquet as pq
    import pyarrow as pa
    from datetime import datetime, timezone

    files = find_parquet_files(root)
    if not files:
        return {"files_scanned": 0, "rows_total": 0, "note": "no parquet files found"}

    rows_total = 0
    g_tmin = None
    g_tmax = None

    for f in files:
        try:
            pf = pq.ParquetFile(str(f))
            rows_total += pf.metadata.num_rows or 0
            # iterate row groups to gather min/max on time_col
            for rg in range(pf.metadata.num_row_groups):
                rgm = pf.metadata.row_group(rg)
                for c in range(rgm.num_columns):
                    cm = rgm.column(c)
                    if cm.path_in_schema == time_col and cm.statistics and cm.statistics.has_min_max:
                        vmin = cm.statistics.min
                        vmax = cm.statistics.max
                        # vmin/vmax may be int (epoch), float, or bytes (INT96)
                        # Try to convert with pyarrow scalar cast
                        try:
                            # if already seconds float
                            if isinstance(vmin, (int, float)):
                                tmin = pd.to_datetime(vmin, unit="s", utc=True)
                                tmax = pd.to_datetime(vmax, unit="s", utc=True)
                            else:
                                # As a fallback, read as numpy via pa.scalar
                                tmin = pd.to_datetime(pa.scalar(vmin).as_py(), utc=True)
                                tmax = pd.to_datetime(pa.scalar(vmax).as_py(), utc=True)
                        except Exception:
                            tmin = tmax = None
                        if tmin is not None:
                            g_tmin = tmin if (g_tmin is None or tmin < g_tmin) else g_tmin
                            g_tmax = tmax if (g_tmax is None or tmax > g_tmax) else g_tmax
        except Exception:
            continue

    span_sec = float((g_tmax - g_tmin).total_seconds()) if (g_tmin is not None and g_tmax is not None) else 0.0
    return {
        "files_scanned": len(files),
        "rows_total": int(rows_total),
        "tmin": g_tmin,
        "tmax": g_tmax,
        "span_sec": span_sec,
        "note": "metadata-only (no data scan)"
    }


def collect_block_maxima_from_metadata(root, latency_col="end_to_end_latency_seconds"):
    """
    Very fast per-file maxima using Parquet footer stats (no data scan).
    Falls back to reading the column if stats missing.
    Returns: np.ndarray of per-file maxima.
    """
    import pyarrow.parquet as pq
    import numpy as np

    files = find_parquet_files(root)
    out = []
    fallback = []

    for f in files:
        try:
            pf = pq.ParquetFile(str(f))
            found = False
            max_vals = []
            for rg in range(pf.metadata.num_row_groups):
                rgm = pf.metadata.row_group(rg)
                for c in range(rgm.num_columns):
                    cm = rgm.column(c)
                    if cm.path_in_schema == latency_col and cm.statistics and cm.statistics.has_min_max:
                        max_vals.append(cm.statistics.max)
                        found = True
                        break
            if found and max_vals:
                try:
                    out.append(float(max(max_vals)))
                except Exception:
                    # stats present but non-numeric? fallback
                    fallback.append(f)
            else:
                fallback.append(f)
        except Exception:
            fallback.append(f)

    if fallback:
        # Slow path only for files without stats
        arr = collect_block_maxima_fixed(fallback, latency_col=latency_col, max_files=None, sample="head")
        out.extend(arr.tolist())

    return np.array(out, dtype=float)


def throughput_from_parquet_dir_dataset(root,
                                        time_col="consumer_receive_timestamp",
                                        size_col="size_bytes",
                                        time_unit="s",
                                        max_files=None,          # kept for signature parity (unused here)
                                        filter_start=None,
                                        filter_end=None):
    """
    Version-tolerant throughput (msgs/s, bytes/s, MB/s) using pyarrow.dataset.
    Works on older pyarrow that do NOT have Dataset.scan(...).

    - Reads only two columns.
    - Optional time filter pushdown if your time_col is numeric epoch seconds.

    Returns: pandas.DataFrame indexed by UTC timestamp seconds with:
             ['msgs_per_sec', 'bytes_per_sec', 'MB_per_sec']
    """
    import pyarrow.dataset as ds
    import pyarrow as pa
    import pyarrow.compute as pc
    import pandas as pd

    dataset = ds.dataset(str(root), format="parquet")

    # Build filter for pushdown (only effective if time_col is numeric seconds)
    filt = None
    def _to_scalar_sec(x):
        if isinstance(x, pd.Timestamp):
            return int(x.value // 10**9)  # ns -> sec
        return x

    if filter_start is not None:
        cond = pc.field(time_col) >= _to_scalar_sec(filter_start)
        filt = cond if filt is None else (filt & cond)
    if filter_end is not None:
        cond = pc.field(time_col) < _to_scalar_sec(filter_end)
        filt = cond if filt is None else (filt & cond)

    # Pull just the needed columns from the dataset
    cols = [time_col, size_col]
    table = None
    # Prefer modern API
    if hasattr(dataset, "to_table"):
        table = dataset.to_table(columns=cols, filter=filt, use_threads=True)
    else:
        # Older pyarrow: build a Scanner explicitly
        scanner = ds.Scanner.from_dataset(dataset, columns=cols, filter=filt, use_threads=True)
        table = scanner.to_table()

    if table.num_rows == 0:
        return pd.DataFrame(columns=["msgs_per_sec", "bytes_per_sec", "MB_per_sec"])

    # Compute integer-second buckets
    col_t = table[time_col]
    if pa.types.is_timestamp(col_t.type):
        # cast timestamp to seconds
        sec = pc.floor_divide(pc.divide(pc.cast(col_t, pa.timestamp("ns")).view("int64"), 10**9), 1)
    else:
        # assume already numeric seconds; cast to int64
        sec = pc.cast(col_t, pa.int64())

    # Build a small pandas frame and aggregate per second
    pdf = pa.table({"sec": sec, "size": table[size_col]}).to_pandas(types_mapper=None)
    if pdf.empty:
        return pd.DataFrame(columns=["msgs_per_sec", "bytes_per_sec", "MB_per_sec"])

    agg_bytes = pdf.groupby("sec", sort=True)["size"].sum()
    agg_msgs  = pdf.groupby("sec", sort=True)["sec"].size()

    out = pd.DataFrame({
        "msgs_per_sec": agg_msgs,
        "bytes_per_sec": agg_bytes
    })
    out["MB_per_sec"] = out["bytes_per_sec"] / (1024.0 * 1024.0)
    out.index.name = "sec"
    out["ts"] = pd.to_datetime(out.index, unit="s", utc=True)
    out = out.set_index("ts").sort_index()
    return out[["msgs_per_sec", "bytes_per_sec", "MB_per_sec"]]
