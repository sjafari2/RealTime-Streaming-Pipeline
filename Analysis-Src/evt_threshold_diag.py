# Create a Jupyter notebook that supports MULTIPLE block_maxima CSVs (and multiple GEV CSVs),
# walking inner folders recursively, with per-root and combined analyses.
#
# It will be saved as /mnt/data/evt_threshold_from_blockmax_MULTI.ipynb

import nbformat as nbf
import os, json
from datetime import datetime

nb = nbf.v4.new_notebook()
cells = []

cells.append(nbf.v4.new_markdown_cell("""
# EVT Threshold Diagnostics — **Multi‑Directory / Multi‑CSV**

You told me (many times 😊) that you always have **more than one file** and **nested folders**.  
This notebook is built for that:
- Accepts **multiple roots** and walks **inner folders recursively**
- Reads **many** `block_maxima.csv` files and **many** `gev_fit_results.csv` files
- Lets you **cap files per root**, **trim warm‑up**, and choose a **threshold grid**
- Produces **combined** diagnostics **and** optional **per‑root** diagnostics

**Outputs**: MRL, ξ‑stability (with GEV overlays if available), per‑threshold GPD QQs, and CSV summaries.
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Parameters =========
# List of ROOT directories; search recursively for matching CSVs under each.
ROOTS = [
    "/mnt/data",  # <-- put each of your 5 (or more) root dirs here
    # "/path/to/dir2",
    # "/path/to/dir3",
    # "/path/to/dir4",
    # "/path/to/dir5",
]

# Filenames to look for (found anywhere under each ROOT, recursively)
BLOCK_MAXIMA_BASENAME = "block_maxima.csv"
GEV_RESULTS_BASENAME  = "gev_fit_results.csv"

# Limits and recursion
RECURSIVE = True
MAX_BLOCK_FILES_PER_ROOT = None   # e.g., 50 (None = read all)
MAX_GEV_FILES_PER_ROOT   = None   # optional cap for GEV result files

# Data handling
THRESHOLDS_S = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
MAX_ROWS     = None   # cap rows per CSV, e.g., 1_000_000
WARMUP_FRAC  = 0.0    # drop first fraction of rows in each CSV if KEEP_WARMUP==False
KEEP_WARMUP  = True   # True: keep warm-up even if WARMUP_FRAC>0

# Column detection (seconds / ms). Set COL_NAME to force a specific column if you want.
COL_NAME = None
SEC_COLS = ['block_max', 'e2e_sec', 'latency_sec', 'e2e_s', 'latency_s', 'value']
MS_COLS  = ['e2e_ms', 'latency_ms']

# Output control
PER_ROOT = True            # also generate diagnostics per root
OUT_DIR  = None            # None => auto timestamped folder
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Imports =========
import os, glob, math, json
from datetime import datetime
from typing import Optional, List, Tuple, Sequence

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import genpareto, kstest

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 160)
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= File discovery & loading =========

def find_files(root: str, basename: str, recursive: bool = True, limit: Optional[int] = None) -> List[str]:
    if recursive:
        pattern = os.path.join(root, "**", basename)
        files = sorted(glob.glob(pattern, recursive=True))
    else:
        pattern = os.path.join(root, basename)
        files = sorted(glob.glob(pattern, recursive=False))
    if limit is not None:
        files = files[:limit]
    return files

def load_from_csv(
    path: str,
    col_name: Optional[str] = None,
    max_rows: Optional[int] = None,
    warmup_frac: float = 0.0,
    keep_warmup: bool = True,
    sec_cols=None,
    ms_cols=None
) -> np.ndarray:
    if sec_cols is None:
        sec_cols = ['block_max', 'e2e_sec', 'latency_sec', 'e2e_s', 'latency_s', 'value']
    if ms_cols is None:
        ms_cols = ['e2e_ms', 'latency_ms']

    df = pd.read_csv(path)
    if max_rows is not None:
        df = df.head(max_rows)

    arr = None
    if col_name and col_name in df.columns:
        arr = df[col_name].to_numpy(dtype=float)
    else:
        for c in sec_cols:
            if c in df.columns:
                arr = df[c].to_numpy(dtype=float); break
        if arr is None:
            for c in ms_cols:
                if c in df.columns:
                    arr = df[c].to_numpy(dtype=float) / 1000.0; break
        if arr is None:
            for c in df.columns:
                if pd.api.types.is_numeric_dtype(df[c]):
                    vals = df[c].to_numpy(dtype=float)
                    with np.errstate(all='ignore'):
                        med = np.nanmedian(vals)
                    if np.isfinite(med) and med > 200:
                        vals = vals / 1000.0
                    arr = vals; break
    if arr is None:
        raise RuntimeError(f"No usable numeric latency column in {path}. Columns: {list(df.columns)}")

    arr = arr[np.isfinite(arr)]
    arr = np.sort(arr)
    if warmup_frac > 0 and not keep_warmup:
        n = len(arr); cut = int(n * warmup_frac)
        if 0 < cut < n: arr = arr[cut:]
    return arr

def load_gev_csv(path: str) -> pd.DataFrame:
    # Expected columns:
    # timestamp, metric, scope, pod, n_blocks, shape_c, xi, location_mu, scale_beta, ks_D, ks_pvalue, config_hash
    try:
        df = pd.read_csv(path)
        return df
    except Exception:
        return pd.DataFrame()
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= EVT helpers =========

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
    plt.scatter(q_theory, y_sorted, s=18)
    lim = max(q_theory.max(), y_sorted.max())
    plt.plot([0, lim], [0, lim])
    plt.xlabel("Theoretical quantiles"); plt.ylabel("Empirical quantiles")
    plt.title(title); plt.tight_layout()
    plt.savefig(out_path, dpi=150); plt.close()
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Output folder =========
if OUT_DIR is None:
    OUT_DIR = os.path.join("/mnt/data", f"evt_from_blockmax_MULTI_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
os.makedirs(OUT_DIR, exist_ok=True)
print("Outputs will be saved under:", OUT_DIR)
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Discover & load data across roots =========

all_block_vals = []
gev_all = []

per_root_files = []

for root in ROOTS:
    bfiles = find_files(root, BLOCK_MAXIMA_BASENAME, recursive=RECURSIVE, limit=MAX_BLOCK_FILES_PER_ROOT)
    gfiles = find_files(root, GEV_RESULTS_BASENAME,  recursive=RECURSIVE, limit=MAX_GEV_FILES_PER_ROOT)
    per_root_files.append((root, bfiles, gfiles))

    # load block maxima arrays
    for bf in bfiles:
        try:
            vals = load_from_csv(bf, col_name=COL_NAME, max_rows=MAX_ROWS,
                                 warmup_frac=WARMUP_FRAC, keep_warmup=KEEP_WARMUP,
                                 sec_cols=SEC_COLS, ms_cols=MS_COLS)
            all_block_vals.append(vals)
        except Exception as e:
            print(f"[WARN] Skipping {bf}: {e}")

    # load GEV CSVs
    for gf in gfiles:
        df = load_gev_csv(gf)
        if not df.empty:
            df["__source_root__"] = root
            df["__source_file__"] = gf
            gev_all.append(df)

if not all_block_vals:
    raise RuntimeError("No block_maxima CSV values loaded. Check your ROOTS and filenames.")

lat_all = np.sort(np.concatenate(all_block_vals))
gev_df_all = pd.concat(gev_all, ignore_index=True) if gev_all else None

print(f"Loaded block maxima arrays from {sum(len(b) for _,b,_ in per_root_files)} files; total N={len(lat_all)}")
if gev_df_all is not None:
    print(f"Loaded GEV tables from {len(gev_all)} files; combined rows={len(gev_df_all)}")
    display(gev_df_all.head())
else:
    print("No GEV CSVs found/loaded.")
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Combined diagnostics =========

# MRL (combined)
mrl_us = np.linspace(min(THRESHOLDS_S), max(THRESHOLDS_S), 25)
mrl_vals = []
for u in mrl_us:
    y = lat_all[lat_all > u] - u
    mrl_vals.append(np.mean(y) if len(y) else np.nan)

plt.figure(figsize=(9, 6))
plt.plot(mrl_us, mrl_vals, marker='o')
plt.xlabel("Threshold u (s)"); plt.ylabel("MRL E[X-u | X>u] (s)")
plt.title("Mean Residual Life (MRL) — Combined")
plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR, "mrl_curve_combined.png"), dpi=150); plt.show()

# Threshold-wise POT fits (combined)
rows_c, xi_us_c, xi_vals_c = [], [], []
for u in THRESHOLDS_S:
    y = lat_all[lat_all > u] - u
    n_exc = len(y)
    if n_exc < 5:
        rows_c.append(dict(u=u, n_exc=n_exc, xi=np.nan, sigma=np.nan, ks_stat=np.nan, ks_p=np.nan, ad_stat=np.nan))
        continue
    xi, sigma = gpd_fit(y)
    ks_stat, ks_p = ks_test_gpd(y, xi, sigma)
    ad_stat = ad_test_gpd(y, xi, sigma)
    xi_us_c.append(u); xi_vals_c.append(xi)
    gpd_qq_plot(y, xi, sigma, f"GPD QQ — Combined u={u:.2f}s (n={n_exc})",
                os.path.join(OUT_DIR, f"qq_gpd_combined_u_{str(u).replace('.','p')}.png"))
    rows_c.append(dict(u=u, n_exc=n_exc, xi=xi, sigma=sigma, ks_stat=ks_stat, ks_p=ks_p, ad_stat=ad_stat))

df_pot_combined = pd.DataFrame(rows_c).sort_values("u")
display(df_pot_combined)
df_pot_combined.to_csv(os.path.join(OUT_DIR, "gpd_threshold_summary_combined.csv"), index=False)

# ξ stability with optional GEV overlay (combined)
plt.figure(figsize=(9, 6))
if xi_us_c:
    plt.plot(xi_us_c, xi_vals_c, marker='o', label="POT ξ (GPD) — Combined")

overlay_loaded = False
if gev_df_all is not None and "xi" in gev_df_all.columns:
    # If there is a 'u' column in GEV CSVs we’ll align; otherwise, show median band.
    if "u" in gev_df_all.columns:
        # Aggregate by u (median) to avoid clutter
        agg = gev_df_all.groupby("u", as_index=False)["xi"].median()
        plt.plot(agg["u"].values, agg["xi"].values, marker='x', linestyle='--', label="GEV ξ median by u (all)")
        overlay_loaded = True
    else:
        xi_med = float(gev_df_all["xi"].median())
        plt.axhline(xi_med, linestyle='--', label=f"GEV ξ median (all) ≈ {xi_med:.3f}")
        overlay_loaded = True

plt.xlabel("Threshold u (s)"); plt.ylabel("Shape ξ")
ttl = "Shape (ξ) stability — Combined POT"
if overlay_loaded: ttl += " + GEV overlay"
plt.title(ttl); plt.legend(loc="best")
plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR, "xi_stability_combined.png"), dpi=150); plt.show()
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Optional: per-root diagnostics =========

import pathlib
if PER_ROOT:
    for root, bfiles, gfiles in per_root_files:
        if not bfiles:
            continue
        out_r = os.path.join(OUT_DIR, f"per_root_{pathlib.Path(root).name}")
        os.makedirs(out_r, exist_ok=True)

        # Load block maxima for this root
        vals_r = []
        for bf in bfiles:
            try:
                arr = load_from_csv(bf, col_name=COL_NAME, max_rows=MAX_ROWS,
                                    warmup_frac=WARMUP_FRAC, keep_warmup=KEEP_WARMUP,
                                    sec_cols=SEC_COLS, ms_cols=MS_COLS)
                vals_r.append(arr)
            except Exception as e:
                print(f"[WARN] Skipping {bf}: {e}")
        if not vals_r:
            continue
        lat_r = np.sort(np.concatenate(vals_r))

        # MRL
        mrl_us = np.linspace(min(THRESHOLDS_S), max(THRESHOLDS_S), 25)
        mrl_vals = [np.mean(lat_r[lat_r > u] - u) if np.any(lat_r > u) else np.nan for u in mrl_us]
        plt.figure(figsize=(9, 6))
        plt.plot(mrl_us, mrl_vals, marker='o')
        plt.xlabel("Threshold u (s)"); plt.ylabel("MRL E[X-u | X>u] (s)")
        plt.title(f"MRL — {root}")
        plt.tight_layout(); plt.savefig(os.path.join(out_r, "mrl_curve.png"), dpi=150); plt.close()

        rows_r, xi_us_r, xi_vals_r = [], [], []
        for u in THRESHOLDS_S:
            y = lat_r[lat_r > u] - u
            n_exc = len(y)
            if n_exc < 5:
                rows_r.append(dict(u=u, n_exc=n_exc, xi=np.nan, sigma=np.nan, ks_stat=np.nan, ks_p=np.nan, ad_stat=np.nan))
                continue
            xi, sigma = gpd_fit(y)
            ks_stat, ks_p = ks_test_gpd(y, xi, sigma)
            ad_stat = ad_test_gpd(y, xi, sigma)
            xi_us_r.append(u); xi_vals_r.append(xi)
            gpd_qq_plot(y, xi, sigma, f"GPD QQ — {pathlib.Path(root).name} u={u:.2f}s (n={n_exc})",
                        os.path.join(out_r, f"qq_gpd_u_{str(u).replace('.','p')}.png"))
            rows_r.append(dict(u=u, n_exc=n_exc, xi=xi, sigma=sigma, ks_stat=ks_stat, ks_p=ks_p, ad_stat=ad_stat))

        df_r = pd.DataFrame(rows_r).sort_values("u")
        df_r.to_csv(os.path.join(out_r, "gpd_threshold_summary.csv"), index=False)

        # ξ stability (per root)
        plt.figure(figsize=(9, 6))
        if xi_us_r:
            plt.plot(xi_us_r, xi_vals_r, marker='o', label="POT ξ (GPD)")
        # GEV overlay per root (aggregate its GEV files if any)
        gdfs = [load_gev_csv(gf) for gf in gfiles]
        gdf = pd.concat([d for d in gdfs if not d.empty], ignore_index=True) if gdfs else None
        overlay_loaded = False
        if gdf is not None and "xi" in gdf.columns and len(gdf) > 0:
            if "u" in gdf.columns:
                agg = gdf.groupby("u", as_index=False)["xi"].median()
                plt.plot(agg["u"].values, agg["xi"].values, marker='x', linestyle='--', label="GEV ξ median by u")
                overlay_loaded = True
            else:
                xi_med = float(gdf["xi"].median())
                plt.axhline(xi_med, linestyle='--', label=f"GEV ξ median ≈ {xi_med:.3f}")
                overlay_loaded = True
        plt.xlabel("Threshold u (s)"); plt.ylabel("Shape ξ")
        ttl = f"Shape (ξ) stability — {pathlib.Path(root).name}"
        if overlay_loaded: ttl += " + GEV overlay"
        plt.title(ttl); plt.legend(loc="best")
        plt.tight_layout(); plt.savefig(os.path.join(out_r, "xi_stability.png"), dpi=150); plt.close()
"""))

cells.append(nbf.v4.new_code_cell("""
# ========= Save manifest =========

manifest = {
    "out_dir": OUT_DIR,
    "roots": ROOTS,
    "generated": datetime.utcnow().isoformat() + "Z",
    "combined": {
        "mrl": "mrl_curve_combined.png",
        "xi_stability": "xi_stability_combined.png",
        "summary_csv": "gpd_threshold_summary_combined.csv",
        "qq_prefix": "qq_gpd_combined_u_*.png"
    },
    "per_root": PER_ROOT
}
with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
    json.dump(manifest, f, indent=2)

print("Manifest written to:", os.path.join(OUT_DIR, "manifest.json"))
print("Done.")
"""))

nb["cells"] = cells
out_path = "/mnt/data/evt_threshold_from_blockmax_MULTI.ipynb"
with open(out_path, "w", encoding="utf-8") as f:
    nbf.write(nb, f)

print("Saved:", out_path)
