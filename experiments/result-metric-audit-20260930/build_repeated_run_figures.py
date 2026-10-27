"""Plot measured rate response and variation between two independent runs.

Usage: python build_repeated_run_figures.py --data evidence --output figures
Requires numpy and matplotlib. The compact input records retain source hashes.
"""

import argparse
import hashlib
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np


def save(fig, destination):
    fig.savefig(str(destination) + ".pdf", bbox_inches="tight")
    fig.savefig(str(destination) + ".png", dpi=190, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "pdf.fonttype": 42, "axes.titleweight": "bold"})
    balanced_path = args.data / "balanced-comparison.json"
    backlog_path = args.data / "backlog-repeats.json"
    balanced = json.loads(balanced_path.read_text())
    backlog = json.loads(backlog_path.read_text())
    rates = sorted(set(r["rate"] for r in balanced))
    summary = {"balanced": {}, "backlog": {}, "inputs": {},
               "method": "Two runs receive equal weight. Sample SD uses ddof=1; bands are not confidence intervals."}
    for path in (balanced_path, backlog_path):
        summary["inputs"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    specs = [("final_ten_min_backlog_growth", "(a) Backlog growth", "Offsets/s"),
             ("p99_seconds", "(b) Completion p99", "Seconds (log scale)"),
             ("unfinished_percent", "(c) Unfinished messages", "Percent")]
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.2))
    for ax, (key, title, ylabel) in zip(axes, specs):
        values = []
        for rate in rates:
            records = sorted([r for r in balanced if r["rate"] == rate], key=lambda r: r["run_number"])
            assert [r["run_number"] for r in records] == [1, 2]
            values.append([r[key] for r in records])
        values = np.array(values)
        means = values.mean(axis=1)
        summary["balanced"][key] = [{"rate": rate, "run_values": list(row),
                                    "mean": float(np.mean(row)), "sample_sd": float(np.std(row, ddof=1))}
                                   for rate, row in zip(rates, values)]
        ax.plot(rates, means, color="#406a91", linewidth=1.3, zorder=1)
        ax.errorbar(rates, means, yerr=[means-values.min(axis=1), values.max(axis=1)-means],
                    fmt="D", color="#406a91", markersize=4, capsize=4, linewidth=1.3, zorder=2)
        # Slight horizontal offsets separate equal-valued runs; they do not change input rates.
        ax.scatter(np.array(rates)-7, values[:, 0], marker="o", facecolors="white", edgecolors="#1b2836", s=24, zorder=3)
        ax.scatter(np.array(rates)+7, values[:, 1], marker="x", color="#b44a32", s=24, zorder=3)
        ax.set(title=title, xlabel="Total input rate (msg/s)", ylabel=ylabel)
        ax.set_xticks(rates)
        ax.tick_params(axis="x", labelrotation=45)
        ax.grid(axis="y", alpha=.2)
        if key == "p99_seconds":
            ax.set_yscale("log")
        else:
            ax.axhline(0, color=".6", linewidth=.6)
    handles = [Line2D([], [], marker="o", color="#1b2836", markerfacecolor="white", ls="", label="Run 1"),
               Line2D([], [], marker="x", color="#b44a32", ls="", label="Run 2"),
               Line2D([], [], marker="D", color="#406a91", label="Mean; whiskers: observed range")]
    fig.legend(handles=handles, loc="upper center", ncol=3, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, .88))
    save(fig, args.output / "balanced-rate-response-repeats")

    arms = [("keep3", "Keep 3", "#555555"),
            ("redistribute3", "Redistribute 3", "#25765c"),
            ("scale_redistribute6", "Targeted scale 6", "#b47824"),
            ("kafka_scale6", "Kafka scale 6", "#356cb0")]
    fig, axes = plt.subplots(2, 2, figsize=(9, 7.4), sharex=True)
    grid = np.arange(0, 600, 2)
    for ax, (arm, title, color) in zip(axes.flat, arms):
        records = sorted([r for r in backlog["runs"] if r["arm"] == arm], key=lambda r: r["run_number"])
        assert [r["run_number"] for r in records] == [1, 2]
        matrix = np.full((2, len(grid)), np.nan)
        for i, record in enumerate(records):
            slots = {}
            for second, value in record["points"]:
                slot = int(round(second / 2))
                assert abs(second-slot*2) <= .001, "Unexpected monitoring grid"
                if not 0 <= slot < len(grid):
                    continue
                slots.setdefault(slot, []).append(value)
            for slot, values in slots.items():
                # The source adds null break markers at ownership changes. Retain
                # those breaks even when a measured value shares the timestamp.
                if any(value is None for value in values):
                    continue
                assert len(set(values)) == 1, "Conflicting values at one grid time"
                value = values[0]
                if value is not None:
                    assert np.isfinite(value) and value >= 0
                    matrix[i, slot] = value
        # No interpolation: a mean or band requires both runs at the same evaluation time.
        valid = np.isfinite(matrix).all(axis=0)
        mean = np.full(len(grid), np.nan)
        sd = np.full(len(grid), np.nan)
        mean[valid] = matrix[:, valid].mean(axis=0)
        sd[valid] = matrix[:, valid].std(axis=0, ddof=1)
        ax.fill_between(grid/60, mean-sd, mean+sd, color=color, alpha=.22)
        ax.plot(grid/60, matrix[0], color=color, linestyle="--", linewidth=.9, alpha=.7)
        ax.plot(grid/60, matrix[1], color=color, linestyle=":", linewidth=.9, alpha=.7)
        ax.plot(grid/60, mean, color=color, linewidth=1.6)
        ax.axvline(1, color=".4", linestyle="--", linewidth=.8)
        ax.set(title=title, ylabel="Total backlog (offsets)", xlim=(0, 10))
        ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0), useMathText=True)
        ax.grid(axis="y", alpha=.2)
        summary["backlog"][arm] = {"run_ids": [r["run_id"] for r in records],
                                   "expected_grid_samples": len(grid), "paired_valid_samples": int(valid.sum()),
                                   "samples": [{"evaluation_second": int(t), "mean": float(m) if ok else None,
                                                "sample_sd": float(s) if ok else None}
                                               for t, m, s, ok in zip(grid, mean, sd, valid)]}
    for ax in axes[-1]:
        ax.set_xlabel("Time since evaluation start (min)")
    fig.legend(handles=[Line2D([], [], color=".3", label="Mean of two runs"),
                        Patch(facecolor=".6", alpha=.3, label="Mean ± sample SD"),
                        Line2D([], [], color=".3", ls="--", label="Run 1"),
                        Line2D([], [], color=".3", ls=":", label="Run 2")],
               loc="upper center", ncol=4, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, .92))
    save(fig, args.output / "twelve-partition-backlog-variation")
    (args.output / "repeated-run-summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False)+"\n")


if __name__ == "__main__":
    main()
