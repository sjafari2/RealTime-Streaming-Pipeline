import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, Sequence, Dict, List

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
