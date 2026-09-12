# evt_triage.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

# --------------------------
# Small helpers
# --------------------------

def _gev_type(c, tol=0.05):
    if np.isnan(c): return "unknown"
    if c > tol:     return "Fréchet (heavy-tailed)"
    if c < -tol:    return "Weibull (upper-bounded)"
    return "Gumbel (light tail)"

def _gpd_type(xi, tol=0.05):
    if np.isnan(xi): return "unknown"
    if xi > tol:     return "heavy (xi>0)"
    if xi < -tol:    return "bounded (xi<0)"
    return "exponential-like (xi≈0)"

def _qqplot(values, dist, params: dict, title: str, out_png: str, show=True, qs=None):
    x = np.asarray(values, float)
    x = x[np.isfinite(x)]
    if x.size < 5 or params is None:
        return
    if qs is None:
        qs = np.linspace(0.01, 0.99, 99)
    emp = np.quantile(x, qs)

    if dist == "gev":
        th = stats.genextreme.ppf(qs, c=params["c"], loc=params["loc"], scale=params["scale"])
    elif dist == "gpd":
        th = stats.genpareto.ppf(qs, c=params["c"], loc=params["loc"], scale=params["scale"])
    elif dist == "trunc-gev":
        # theoretical quantiles of truncated GEV on [min, u]:
        # F_tr^{-1}(q) = F^{-1}(q*F(u))
        c, loc, scale, u = params["c"], params["loc"], params["scale"], params["u"]
        Fu = stats.genextreme.cdf(u, c=c, loc=loc, scale=scale)
        th = stats.genextreme.ppf(qs * Fu, c=c, loc=loc, scale=scale)
    else:
        return

    fig, ax = plt.subplots()
    ax.plot(th, emp, '.', label="Empirical vs Theoretical")
    lo = min(th.min(), emp.min()); hi = max(th.max(), emp.max())
    ax.plot([lo, hi], [lo, hi], '-', label="y=x")
    ax.set_xlabel("Theoretical quantiles")
    ax.set_ylabel("Empirical quantiles")
    ax.set_title(title)
    h,l = ax.get_legend_handles_labels()
    if h and any(lbl and not str(lbl).startswith("_") for lbl in l):
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    if show: plt.show()
    plt.close(fig)

def _ks_asym(values, cdf_callable):
    x = np.asarray(values, float)
    x = x[np.isfinite(x)]
    if x.size < 5:
        return np.nan, np.nan, int(x.size)
    stat, p = stats.kstest(x, cdf_callable)
    return float(stat), float(p), int(x.size)

def _bootstrap_p(D_obs, sampler, cdf_callable, n, B=400, jitter=0.0, rng=None):
    if rng is None:
        rng = np.random.default_rng(0)
    Db = np.empty(B, float)
    for b in range(B):
        xb = sampler(n, rng)
        if jitter and n>0:
            xb = xb + rng.normal(0.0, float(jitter), size=n)
        d, _ = stats.kstest(xb, cdf_callable)
        Db[b] = d
    p_boot = float(np.mean(Db >= D_obs))
    return p_boot, float(Db.mean())

# --------------------------
# 1) FULL GEV on all maxima
# --------------------------

def analyze_full_gev(experiments, out_dir, B=400, show=True, jitter=0.0, seed=42):
    rows = []
    rng = np.random.default_rng(seed)

    for e in experiments:
        label = e["label"]
        vals  = np.asarray(e["maxima"], float)
        p     = e["gev_params"]  # dict(c, loc, scale)

        # KS (asymptotic) vs fitted GEV
        c, loc, scale = p["c"], p["loc"], max(p["scale"], 1e-9)
        cdf = lambda t: stats.genextreme.cdf(t, c=c, loc=loc, scale=scale)
        D, p_asym, n = _ks_asym(vals, cdf)

        # Bootstrap p-value (simulate from fitted GEV, refit on each bootstrap)
        def sampler(n_boot, rng_inner):
            xb = stats.genextreme.rvs(c=c, loc=loc, scale=scale, size=n, random_state=rng_inner)
            cb, locb, scaleb = stats.genextreme.fit(xb)
            scaleb = max(scaleb, 1e-9)
            return stats.genextreme.rvs(c=cb, loc=locb, scale=scaleb, size=n, random_state=rng_inner)

        p_boot, Dm = _bootstrap_p(D, sampler, cdf, n, B=B, jitter=jitter, rng=rng)

        gev_family = _gev_type(c)

        _qqplot(vals, "gev", dict(c=c, loc=loc, scale=scale),
                title=f"GEV QQ – {label} (full)",
                out_png=os.path.join(out_dir, f"qq_full_gev_{label.replace('=','_')}.png"),
                show=show)

        rows.append(dict(
            which="full_gev",
            label=label,
            n=n,
            gev_c=c, gev_loc=loc, gev_scale=scale, gev_type=gev_family,
            KS_D=D, KS_p_asym=p_asym, KS_p_boot=p_boot, KS_D_boot_mean=Dm
        ))

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "diag_full_gev.csv"), index=False)
    return df

# ------------------------------------------
# 2) TRUNCATED GEV on body X<=u (no refit)
# ------------------------------------------

def analyze_truncated_gev(experiments, out_dir, u=1.0, B=400, show=True, jitter=0.0, seed=43):
    rows = []
    rng = np.random.default_rng(seed)

    for e in experiments:
        label = e["label"]
        vals  = np.asarray(e["maxima"], float)
        p     = e["gev_params"]
        c, loc, scale = p["c"], p["loc"], max(p["scale"], 1e-9)

        # Keep body only
        x = vals[np.isfinite(vals) & (vals <= u)]
        n = x.size
        if n < 5:
            rows.append(dict(which="trunc_gev", label=label, n=n,
                             gev_c=c, gev_loc=loc, gev_scale=scale, F_u=np.nan,
                             KS_D=np.nan, KS_p_asym=np.nan, KS_p_boot=np.nan, KS_D_boot_mean=np.nan))
            continue

        if jitter:
            x = x + rng.normal(0.0, float(jitter), size=n)

        Fu  = stats.genextreme.cdf(u, c=c, loc=loc, scale=scale)
        cdf_tr = lambda t: np.clip(stats.genextreme.cdf(t, c=c, loc=loc, scale=scale) / Fu, 0.0, 1.0)
        D, p_asym, _ = _ks_asym(x, cdf_tr)

        # Bootstrap for truncated model
        def sampler(n_boot, rng_inner):
            out = []
            need = n
            while len(out) < n:
                k = int(np.ceil(need / max(Fu, 1e-12)))
                xb = stats.genextreme.rvs(c=c, loc=loc, scale=scale, size=k, random_state=rng_inner)
                xb = xb[xb <= u]
                out.extend(xb.tolist())
                need = n - len(out)
            return np.array(out[:n], float)

        p_boot, Dm = _bootstrap_p(D, sampler, cdf_tr, n, B=B, jitter=jitter, rng=rng)

        _qqplot(x, "trunc-gev", dict(c=c, loc=loc, scale=scale, u=u),
                title=f"Truncated GEV QQ – {label} (≤{u}s)",
                out_png=os.path.join(out_dir, f"qq_trunc_gev_{label.replace('=','_')}.png"),
                show=show)

        rows.append(dict(
            which="trunc_gev",
            label=label,
            n=n,
            gev_c=c, gev_loc=loc, gev_scale=scale, F_u=Fu,
            KS_D=D, KS_p_asym=p_asym, KS_p_boot=p_boot, KS_D_boot_mean=Dm
        ))

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "diag_trunc_gev.csv"), index=False)
    return df

# -----------------------------------
# 3) GPD on exceedances X>u (tail)
# -----------------------------------

def analyze_gpd_tail(experiments, out_dir, u=1.0, B=400, show=True, jitter=0.0, seed=44):
    rows = []
    rng = np.random.default_rng(seed)

    for e in experiments:
        label = e["label"]
        vals  = np.asarray(e["maxima"], float)
        thr   = float(u)

        exc = vals[np.isfinite(vals) & (vals > thr)] - thr
        n = exc.size
        if n < 10:
            rows.append(dict(which="gpd_tail", label=label, n=n,
                             gpd_c=np.nan, gpd_loc=np.nan, gpd_scale=np.nan, gpd_type="n/a",
                             KS_D=np.nan, KS_p_asym=np.nan, KS_p_boot=np.nan, KS_D_boot_mean=np.nan))
            continue

        if jitter:
            exc = exc + rng.normal(0.0, float(jitter), size=n)

        # Fit GPD on exceedances (loc=0)
        c, loc, scale = stats.genpareto.fit(exc, floc=0.0)
        scale = max(scale, 1e-12)
        gpd_family = _gpd_type(c)

        cdf = lambda t: stats.genpareto.cdf(t, c=c, loc=0.0, scale=scale)
        D, p_asym, _ = _ks_asym(exc, cdf)

        # Bootstrap with refit on each replicate
        def sampler(n_boot, rng_inner):
            xb = stats.genpareto.rvs(c=c, loc=0.0, scale=scale, size=n, random_state=rng_inner)
            cb, locb, scaleb = stats.genpareto.fit(xb, floc=0.0)
            scaleb = max(scaleb, 1e-12)
            return stats.genpareto.rvs(c=cb, loc=0.0, scale=scaleb, size=n, random_state=rng_inner)

        p_boot, Dm = _bootstrap_p(D, sampler, cdf, n, B=B, jitter=jitter, rng=rng)

        _qqplot(exc, "gpd", dict(c=c, loc=0.0, scale=scale),
                title=f"GPD QQ – {label} (exceedances > {u}s)",
                out_png=os.path.join(out_dir, f"qq_gpd_{label.replace('=','_')}.png"),
                show=show)

        rows.append(dict(
            which="gpd_tail",
            label=label,
            n=n,
            gpd_c=c, gpd_loc=0.0, gpd_scale=scale, gpd_type=gpd_family,
            KS_D=D, KS_p_asym=p_asym, KS_p_boot=p_boot, KS_D_boot_mean=Dm
        ))

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out_dir, "diag_gpd_tail.csv"), index=False)
    return df

# -----------------------------------
# One-click orchestrator
# -----------------------------------

def run_all_three(experiments, out_dir, u=1.0, B=400, show=True, jitter=1e-6):
    os.makedirs(out_dir, exist_ok=True)
    full_df  = analyze_full_gev(experiments, out_dir, B=B, show=show, jitter=jitter)
    trunc_df = analyze_truncated_gev(experiments, out_dir, u=u, B=B, show=show, jitter=jitter)
    gpd_df   = analyze_gpd_tail(experiments, out_dir, u=u, B=B, show=show, jitter=jitter)

    out = (full_df[["label","gev_c","gev_loc","gev_scale","gev_type","KS_D","KS_p_asym","KS_p_boot"]]
           .rename(columns=lambda k: f"full_{k}" if k not in ["label"] else k)
           .merge(trunc_df[["label","F_u","KS_D","KS_p_asym","KS_p_boot"]]
                  .rename(columns={"F_u":"trunc_Fu",
                                   "KS_D":"trunc_KS_D",
                                   "KS_p_asym":"trunc_KS_p_asym",
                                   "KS_p_boot":"trunc_KS_p_boot"}), on="label", how="outer")
           .merge(gpd_df[["label","gpd_c","gpd_scale","gpd_type","KS_D","KS_p_asym","KS_p_boot"]]
                  .rename(columns={"KS_D":"gpd_KS_D",
                                   "KS_p_asym":"gpd_KS_p_asym",
                                   "KS_p_boot":"gpd_KS_p_boot"}), on="label", how="outer")
          )
    out_csv = os.path.join(out_dir, "evt_triage_summary.csv")
    out.to_csv(out_csv, index=False)
    return out_csv
# --- FAST BOOTSTRAP PATCHES FOR evt_triage ---
import types, numpy as np
from scipy import stats

def analyze_full_gev_fast(experiments, out_dir, B=100, show=False, jitter=0.0, seed=42):
    import pandas as pd, os, matplotlib.pyplot as plt
    rows=[]; rng=np.random.default_rng(seed)
    for e in experiments:
        label=e["label"]; vals=np.asarray(e["maxima"],float); p=e["gev_params"]
        c,loc,scale=p["c"],p["loc"],max(p["scale"],1e-9)
        cdf=lambda t: stats.genextreme.cdf(t,c=c,loc=loc,scale=scale)
        # KS on observed
        x=vals[np.isfinite(vals)]
        if x.size<5:
            rows.append(dict(which="full_gev",label=label,n=x.size,gev_c=c,gev_loc=loc,gev_scale=scale,
                             gev_type=("Fréchet (heavy-tailed)" if c>0.05 else "Weibull (upper-bounded)" if c<-0.05 else "Gumbel (light tail)"),
                             KS_D=np.nan,KS_p_asym=np.nan,KS_p_boot=np.nan,KS_D_boot_mean=np.nan))
            continue
        if jitter: x=x+rng.normal(0.0,float(jitter),size=x.size)
        from scipy.stats import kstest
        D,p_asym=kstest(x,cdf)
        # FAST bootstrap: no refit, just fixed-parameter resamples
        Db=np.empty(B,float)
        for b in range(B):
            xb=stats.genextreme.rvs(c=c,loc=loc,scale=scale,size=x.size,random_state=rng)
            if jitter: xb=xb+rng.normal(0.0,float(jitter),size=xb.size)
            d,_=kstest(xb,cdf)
            Db[b]=d
        p_boot=float(np.mean(Db>=D)); Dm=float(Db.mean())
        # QQ
        qs=np.linspace(0.01,0.99,99)
        emp=np.quantile(x,qs); th=stats.genextreme.ppf(qs,c=c,loc=loc,scale=scale)
        fig,ax=plt.subplots(); ax.plot(th,emp,'.'); lo=min(th.min(),emp.min()); hi=max(th.max(),emp.max())
        ax.plot([lo,hi],[lo,hi],'-'); ax.set_xlabel("Theoretical"); ax.set_ylabel("Empirical")
        ax.set_title(f"GEV QQ – {label} (full)")
        fig.tight_layout(); fig.savefig(os.path.join(out_dir,f"qq_full_gev_{label.replace('=','_')}.png"),dpi=150)
        if show: plt.show(); plt.close(fig)
        rows.append(dict(which="full_gev",label=label,n=x.size,gev_c=c,gev_loc=loc,gev_scale=scale,
                         gev_type=("Fréchet (heavy-tailed)" if c>0.05 else "Weibull (upper-bounded)" if c<-0.05 else "Gumbel (light tail)"),
                         KS_D=float(D),KS_p_asym=float(p_asym),KS_p_boot=p_boot,KS_D_boot_mean=Dm))
    import pandas as pd
    df=pd.DataFrame(rows); df.to_csv(os.path.join(out_dir,"diag_full_gev.csv"),index=False); return df

def analyze_truncated_gev_fast(experiments, out_dir, u=1.0, B=100, show=False, jitter=0.0, seed=43):
    import pandas as pd, os, matplotlib.pyplot as plt
    rows=[]; rng=np.random.default_rng(seed)
    for e in experiments:
        label=e["label"]; vals=np.asarray(e["maxima"],float); p=e["gev_params"]
        c,loc,scale=p["c"],p["loc"],max(p["scale"],1e-9)
        x=vals[np.isfinite(vals)&(vals<=u)]; n=x.size
        if n<5:
            rows.append(dict(which="trunc_gev",label=label,n=n,gev_c=c,gev_loc=loc,gev_scale=scale,
                             F_u=np.nan,KS_D=np.nan,KS_p_asym=np.nan,KS_p_boot=np.nan,KS_D_boot_mean=np.nan))
            continue
        if jitter: x=x+rng.normal(0.0,float(jitter),size=n)
        Fu=stats.genextreme.cdf(u,c=c,loc=loc,scale=scale)
        cdf_tr=lambda t: np.clip(stats.genextreme.cdf(t,c=c,loc=loc,scale=scale)/Fu,0.0,1.0)
        from scipy.stats import kstest
        D,p_asym=kstest(x,cdf_tr)
        # FAST bootstrap for truncated: inverse-CDF sampling U~Uniform(0,Fu), X=F^{-1}(U)
        Db=np.empty(B,float)
        for b in range(B):
            U=rng.random(n)*Fu
            xb=stats.genextreme.ppf(U,c=c,loc=loc,scale=scale)
            if jitter: xb=xb+rng.normal(0.0,float(jitter),size=n)
            d,_=kstest(xb,cdf_tr)
            Db[b]=d
        p_boot=float(np.mean(Db>=D)); Dm=float(Db.mean())
        # QQ (truncated): th = F^{-1}(q*Fu)
        qs=np.linspace(0.01,0.99,99); emp=np.quantile(x,qs); th=stats.genextreme.ppf(qs*Fu,c=c,loc=loc,scale=scale)
        fig,ax=plt.subplots(); ax.plot(th,emp,'.'); lo=min(th.min(),emp.min()); hi=max(th.max(),emp.max())
        ax.plot([lo,hi],[lo,hi],'-'); ax.set_xlabel("Theoretical"); ax.set_ylabel("Empirical")
        ax.set_title(f"Truncated GEV QQ – {label} (≤{u}s)")
        fig.tight_layout(); fig.savefig(os.path.join(out_dir,f"qq_trunc_gev_{label.replace('=','_')}.png"),dpi=150)
        if show: plt.show(); plt.close(fig)
        rows.append(dict(which="trunc_gev",label=label,n=n,gev_c=c,gev_loc=loc,gev_scale=scale,F_u=float(Fu),
                         KS_D=float(D),KS_p_asym=float(p_asym),KS_p_boot=p_boot,KS_D_boot_mean=Dm))
    import pandas as pd
    df=pd.DataFrame(rows); df.to_csv(os.path.join(out_dir,"diag_trunc_gev.csv"),index=False); return df

def analyze_gpd_tail_fast(experiments, out_dir, u=1.0, B=100, show=False, jitter=0.0, seed=44):
    import pandas as pd, os, matplotlib.pyplot as plt
    rows=[]; rng=np.random.default_rng(seed)
    for e in experiments:
        label=e["label"]; vals=np.asarray(e["maxima"],float); thr=float(u)
        exc=vals[np.isfinite(vals)&(vals>thr)]-thr; n=exc.size
        if n<10:
            rows.append(dict(which="gpd_tail",label=label,n=n,gpd_c=np.nan,gpd_loc=np.nan,gpd_scale=np.nan,gpd_type="n/a",
                             KS_D=np.nan,KS_p_asym=np.nan,KS_p_boot=np.nan,KS_D_boot_mean=np.nan))
            continue
        if jitter: exc=exc+rng.normal(0.0,float(jitter),size=n)
        # Fit GPD and test
        c,loc,scale=stats.genpareto.fit(exc,floc=0.0); scale=max(scale,1e-12)
        cdf=lambda t: stats.genpareto.cdf(t,c=c,loc=0.0,scale=scale)
        from scipy.stats import kstest
        D,p_asym=kstest(exc,cdf)
        # FAST bootstrap: fixed-parameter resamples (no refit)
        Db=np.empty(B,float)
        for b in range(B):
            xb=stats.genpareto.rvs(c=c,loc=0.0,scale=scale,size=n,random_state=rng)
            if jitter: xb=xb+rng.normal(0.0,float(jitter),size=n)
            d,_=kstest(xb,cdf); Db[b]=d
        p_boot=float(np.mean(Db>=D)); Dm=float(Db.mean())
        # QQ
        qs=np.linspace(0.01,0.99,99); emp=np.quantile(exc,qs); th=stats.genpareto.ppf(qs,c=c,loc=0.0,scale=scale)
        fig,ax=plt.subplots(); ax.plot(th,emp,'.'); lo=min(th.min(),emp.min()); hi=max(th.max(),emp.max())
        ax.plot([lo,hi],[lo,hi],'-'); ax.set_xlabel("Theoretical"); ax.set_ylabel("Empirical")
        ax.set_title(f"GPD QQ – {label} (exceedances > {u}s)")
        fig.tight_layout(); fig.savefig(os.path.join(out_dir,f"qq_gpd_{label.replace('=','_')}.png"),dpi=150)
        if show: plt.show(); plt.close(fig)
        rows.append(dict(which="gpd_tail",label=label,n=n,gpd_c=float(c),gpd_loc=0.0,gpd_scale=float(scale),
                         gpd_type=("heavy (xi>0)" if c>0.05 else "bounded (xi<0)" if c<-0.05 else "exponential-like (xi≈0)"),
                         KS_D=float(D),KS_p_asym=float(p_asym),KS_p_boot=p_boot,KS_D_boot_mean=Dm))
    import pandas as pd
    df=pd.DataFrame(rows); df.to_csv(os.path.join(out_dir,"diag_gpd_tail.csv"),index=False); 
    return df

def run_all_three_fast(experiments, out_dir, u=1.0, B=100, show=False, jitter=1e-6):
    os.makedirs(out_dir, exist_ok=True)
    df1=analyze_full_gev_fast(experiments,out_dir,B=B,show=show,jitter=jitter)
    df2=analyze_truncated_gev_fast(experiments,out_dir,u=u,B=B,show=show,jitter=jitter)
    df3=analyze_gpd_tail_fast(experiments,out_dir,u=u,B=B,show=show,jitter=jitter)
    # merge summary like original
    out=(df1[["label","gev_c","gev_loc","gev_scale","gev_type","KS_D","KS_p_asym","KS_p_boot"]]
         .rename(columns=lambda k: f"full_{k}" if k!="label" else k)
         .merge(df2[["label","F_u","KS_D","KS_p_asym","KS_p_boot"]]
                .rename(columns={"F_u":"trunc_Fu","KS_D":"trunc_KS_D","KS_p_asym":"trunc_KS_p_asym","KS_p_boot":"trunc_KS_p_boot"}),on="label",how="outer")
         .merge(df3[["label","gpd_c","gpd_scale","gpd_type","KS_D","KS_p_asym","KS_p_boot"]]
                .rename(columns={"KS_D":"gpd_KS_D","KS_p_asym":"gpd_KS_p_asym","KS_p_boot":"gpd_KS_p_boot"}),on="label",how="outer"))
    out_csv=os.path.join(out_dir,"evt_triage_summary.csv"); out.to_csv(out_csv,index=False); return out_csv



