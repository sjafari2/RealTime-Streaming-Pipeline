"""Plot the scheduling sensitivity of the synthetic transition audit."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main(folder):
    data = json.loads((folder / "results.json").read_text())
    cases = [x for x in data["queue_sensitivity"] if x["true_capacity"] == 300 and x["initial_age"] == 0]
    fig, axes = plt.subplots(2, 3, figsize=(11, 6.7), constrained_layout=True)
    measures = [("p99_completed_by_cutoff_seconds", "Completion p99 by cutoff (s)"),
                ("unfinished_by_cutoff", "Initial cohort unfinished (%)"),
                ("deadline_misses_known_by_cutoff", "Initial cohort deadline misses (%)")]
    for row, case in enumerate(cases):
        for col, (key, title) in enumerate(measures):
            ax = axes[row, col]
            values = [case["outcomes"][arm][key] for arm in ("keep3", "paper_rule")]
            if col:
                values = [v / 2900 * 100 for v in values]
            bars = ax.bar(["Keep 3", "Rule: scale to 4"], values, color=["#31688e", "#d68932"], width=.58)
            ax.bar_label(bars, labels=[f"{v:.2f}" for v in values], padding=4, fontsize=10)
            ax.set_title(title, fontsize=11)
            ax.spines[["top", "right"]].set_visible(False)
            ax.grid(axis="y", alpha=.2); ax.set_axisbelow(True)
            if not col:
                label = "Oldest-message-first" if row == 0 else "Partition round-robin"
                ax.set_ylabel(label, fontsize=12)
    for col, (key, _) in enumerate(measures):
        largest = max(case["outcomes"][arm][key] for case in cases for arm in ("keep3", "paper_rule"))
        if col:
            largest = largest / 2900 * 100
        for row in range(2):
            axes[row, col].set_ylim(0, largest * 1.3 + .5)
    fig.suptitle("Synthetic 80/20 workload: scheduling changes the decision", fontsize=15)
    fig.supxlabel("600 messages/s; 2,900 initially queued; 300 messages/s per consumer; 5-second action pause\n"
                  "Common cutoff: 12 seconds after decision; message deadline: 10 seconds. Not Nautilus results.", fontsize=10)
    fig.savefig(folder / "scheduling-sensitivity.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    main(parser.parse_args().folder)
