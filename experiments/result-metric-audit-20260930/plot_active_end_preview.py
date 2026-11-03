"""Preview recorded Active–end intervals beneath the twelve-partition traces."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/stream-active-end-plot-cache")

ARMS = ["keep3", "redistribute3", "scale_redistribute6", "kafka_scale6"]
LABELS = ["Keep 3", "Redistribute 3", "Targeted scale 6", "Kafka scale 6"]
COLORS = ["#2563a6", "#dc862d", "#21875e", "#9a4eaa"]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def render(repository, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MultipleLocator, MaxNLocator, StrMethodFormatter

    data_path = repository / "experiment-records/runtime-metric-categories-20261102/metric-category-plot-data.json"
    windows_path = repository / "experiment-records/four-condition-20260929/action-window-performance.json"
    comparison_path = repository / "experiment-records/four-condition-20260929/comparison.json"
    data = json.loads(data_path.read_text())
    windows = json.loads(windows_path.read_text())
    comparison = json.loads(comparison_path.read_text())
    assert digest(comparison_path) == windows["comparison_sha256"]
    assert digest(comparison_path) == data["comparison_sha256"][str(comparison_path.relative_to(repository))]
    rows = [r for r in data["rows"] if r["campaign"] == "twelve700"]
    assert len(rows) == len({r["run_id"] for r in rows}) == 8
    by_id = {r["run_id"]: r for r in windows["runs"]}
    comparison_by_id = {r["run_id"]: r for r in comparison["runs"]}
    audit = []
    for row in rows:
        saved = by_id[row["run_id"]]
        assert saved["arm"] == row["arm"] and saved["run_number"] == row["run"]
        assert saved["event_file_sha256"] == row["event_sha256"]
        assert saved["whole_run_replay_matches"] is True
        assert saved["evidence_sha256"]["manifest.json"] == row["source_hashes"]["manifest.json"]
        boundary = saved["post_action_boundary"]
        interval = saved["windows"]["post_action_to_evaluation_end"]
        start = boundary["after_evaluation_seconds"]
        end = interval["end_after_evaluation_seconds"]
        assert 60 <= start < end == 600
        assert math.isclose(start, interval["start_after_evaluation_seconds"], abs_tol=1e-9)
        assert math.isclose(end - start, interval["duration_seconds"], abs_tol=1e-9)
        if row["arm"] == "keep3":
            assert start == 60 and boundary["basis"] == "scheduled reference; no action"
        else:
            assert math.isclose(boundary["epoch"], comparison_by_id[row["run_id"]]["intervention_cost"]["transition_end_epoch"], abs_tol=1e-6)
        audit.append(dict(run_id=row["run_id"], arm=row["arm"], run=row["run"],
                          start_seconds=start, end_seconds=end, basis=boundary["basis"],
                          boundary_evidence_sha256=boundary["evidence_sha256"]))

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "svg.fonttype": "none", "axes.labelsize": 11})
    fig = plt.figure(figsize=(12, 9.5))
    grid = fig.add_gridspec(4, 2, height_ratios=[1, 1, 1, .87],
                           left=.105, right=.98, bottom=.18, top=.825,
                           hspace=.27, wspace=.17)
    axes = [[fig.add_subplot(grid[i, j]) for j in range(2)] for i in range(4)]
    fig.suptitle("Performance Evaluation Metrics", y=.975, fontsize=16, weight="bold")
    fig.text(.5, .939, "12 high-input partitions | 700 messages/s | recorded Active–end intervals", ha="center", fontsize=11)
    fig.legend(handles=[Line2D([], [], color=c, lw=2, label=l) for c, l in zip(COLORS, LABELS)],
               loc="upper center", bbox_to_anchor=(.5, .918), ncol=4, frameon=False)
    metrics = [("processing_backlog", "Processing backlog\n(offsets)"),
               ("throughput", "Throughput\n(msg/s)"),
               ("window_p99", "Window p99\nend-to-end latency (s)")]
    for i, (key, label) in enumerate(metrics):
        values = [v for r in rows for _, v in r[key] if v is not None]
        assert values and all(math.isfinite(v) for v in values)
        for j in range(2):
            ax = axes[i][j]
            for row in [r for r in rows if r["run"] == j + 1]:
                points = row[key]
                ax.plot([t / 60 for t, _ in points], [math.nan if v is None else v for _, v in points],
                        color=COLORS[ARMS.index(row["arm"])], lw=1.5)
            ax.set(xlim=(0, 10), ylim=(0, max(values) * 1.08))
            ax.axvline(1, color="#333333", ls=":", lw=1)
            ax.axvspan(.5, 1, color="#d8dee5", alpha=.4, zorder=0)
            ax.axvspan(9.5, 10, color="#d8dee5", alpha=.4, zorder=0)
            ax.xaxis.set_major_locator(MultipleLocator(1))
            ax.yaxis.set_major_locator(MultipleLocator(500) if key == "throughput" else MaxNLocator(5))
            ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
            if key == "throughput":
                ax.axhline(700, color="#777777", ls=":", lw=.8)
            ax.spines[["top", "right"]].set_visible(False)
            ax.grid(alpha=.18)
            ax.set_axisbelow(True)
            ax.tick_params(axis="x", labelbottom=False)
            if j == 0:
                ax.set_ylabel(label)
            else:
                ax.tick_params(axis="y", labelleft=False)
            if i == 0:
                ax.set_title("Run " + str(j + 1), fontsize=12, pad=20)
                for x, name in [(.75, "Before"), (9.75, "Eval. end")]:
                    ax.text(x, 1.025, name, transform=ax.get_xaxis_transform(), ha="center", fontsize=9, color="#62676d")

    for j in range(2):
        ax = axes[3][j]
        ax.set(xlim=(0, 10), ylim=(-.6, 3.8), yticks=[])
        ax.set_title("Active–end intervals", fontsize=11, loc="left", pad=6)
        ax.axvline(1, color="#333333", ls=":", lw=1)
        for index, arm in enumerate(ARMS):
            item = next(r for r in audit if r["run"] == j + 1 and r["arm"] == arm)
            start = item["start_seconds"] / 60
            y = 3 - index
            color = COLORS[index]
            ax.plot([start, 10], [y, y], color=color, alpha=.65, lw=3,
                    ls="--" if arm == "keep3" else "-", solid_capstyle="butt")
            ax.plot(start, y, marker="o", ms=5, color=color,
                    markerfacecolor="white" if arm == "keep3" else color, zorder=4)
            label = LABELS[index] + " · " + format(item["start_seconds"], ".1f") + " s"
            if arm == "keep3":
                label += " (reference)"
            ax.text(start + .15, y + .12, label, fontsize=8.5, color="#333333", va="bottom")
            ax.plot([10, 10], [y - .09, y + .09], color=color, lw=1.2, clip_on=False)
        ax.spines[["top", "left", "right"]].set_visible(False)
        ax.xaxis.set_major_locator(MultipleLocator(1))
        ax.set_xlabel("Evaluation time (min)")

    fig.text(.105, .072, "Dotted line: action requested at 60 s. Filled circles: confirmed activation; bars continue to evaluation end (600 s).\n"
             "Keep 3 has no intervention: its open circle marks the comparison reference. Active–end can include backlog recovery.", fontsize=9)
    fig.text(.105, .024, "Shading: Before (30–60 s) and Eval. end (570–600 s). Throughput and p99 use consecutive 30-second intervals.\n"
             "P99 uses evaluation messages completed in each interval. Warm-up completions can contribute to throughput; drain is excluded.", fontsize=8.5, color="#555555")
    output.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "svg"):
        path = output / ("twelve-partition-active-end-preview." + ext)
        fig.savefig(path, dpi=180, facecolor="white")
        if ext == "svg":
            path.write_text("\n".join(s.rstrip() for s in path.read_text().splitlines()) + "\n")
    plt.close(fig)
    (output / "validation.json").write_text(json.dumps(dict(
        source_sha256={str(p.relative_to(repository)): digest(p) for p in (data_path, windows_path, comparison_path)},
        note="Existing time series are unchanged; added bars use the saved unrounded activation boundaries.",
        intervals=audit), indent=2) + "\n")
    print("Verified and rendered 8 recorded intervals; all end at 600 seconds.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    render(args.repository, args.output)
