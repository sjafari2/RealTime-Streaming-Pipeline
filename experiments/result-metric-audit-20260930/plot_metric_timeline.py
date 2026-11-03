"""Draw the measurement schedule for ten-minute mitigation trials.

Activation and recovery positions are illustrative. This diagram describes
measurement boundaries, not the measured outcome of a particular trial.
"""
import argparse
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/stream-metric-mpl")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle


def draw(output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    width, height = 2200, 1290
    plt.rcParams.update({"font.family": "DejaVu Sans", "svg.fonttype": "none"})
    fig = plt.figure(figsize=(22, 12.9), dpi=150, facecolor="white")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set(xlim=(0, width), ylim=(height, 0))
    ax.axis("off")
    ink, muted, grid = "#1E3049", "#566B83", "#DDE4ED"
    blue, teal, purple = "#2368AA", "#0A8177", "#744AB4"
    orange, red = "#AD6516", "#B34650"

    def x(seconds):
        return 500 + (seconds + 60) * 1640 / 780

    def text(a, b, value, size=15, color=ink, bold=False, align="left"):
        return ax.text(a, b, value, fontsize=size, color=color,
                       fontweight="bold" if bold else "normal",
                       ha=align, va="center", linespacing=1.2)

    def line(a, b, c, d, color=grid, style="-", lw=1):
        ax.plot([a, c], [b, d], color=color, linestyle=style,
                linewidth=lw, zorder=0)

    def arrow(a, b, c, d, color=ink, style="-"):
        ax.annotate("", xy=(c, d), xytext=(a, b),
                    arrowprops={"arrowstyle": "-|>", "color": color,
                                "lw": 1.1, "linestyle": style})

    def span(start, stop, y, color, fill, label="", dashed=False):
        ax.add_patch(FancyBboxPatch(
            (x(start), y - 13), x(stop) - x(start), 26,
            boxstyle="round,pad=0,rounding_size=4", facecolor=fill,
            edgecolor=color, linewidth=1, linestyle="--" if dashed else "-"))
        if label:
            text((x(start) + x(stop)) / 2, y, label, 14, color, align="center")

    def row(y, title, note):
        text(45, y - 7, title, 15, bold=True)
        text(45, y + 14, note, 11.5, muted)

    text(45, 38, "Experiment phases and metric windows", 28, bold=True)
    text(45, 78, "One shared ruler: 1 min warm-up + 10 min evaluation + 2 min drain = 13 min", 17, muted)
    phases = [(-60, 0, "#FFF1D8", orange, "Warm-up\n1 min"),
              (0, 600, "#E9F1FB", blue, "Evaluation: 10 min"),
              (600, 720, "#E0F3EE", teal, "Drain: 2 min")]
    for start, stop, fill, color, label in phases:
        ax.add_patch(Rectangle((x(start), 125), x(stop) - x(start), 58,
                               facecolor=fill, edgecolor=color, linewidth=1.2))
        text((x(start) + x(stop)) / 2, 154, label,
             12 if start < 0 else 17, color, True, "center")
    text(45, 154, "Readiness checks precede warm-up.", 14, muted)
    text(x(60) + 18, 106, "Action request", 13, purple, True)
    arrow(x(60), 107, x(60), 125, purple)
    text(45, 207, "Elapsed run time (minutes)", 14, bold=True)
    text(45, 240, "Time since evaluation began (seconds)", 14, bold=True)
    for minute in range(14):
        t = minute * 60 - 60
        line(x(t), 183, x(t), 192, ink)
        text(x(t), 207, str(minute), 14, align="center")
        text(x(t), 240, str(t).replace("-", "−"), 12.5, muted, align="center")
    for t in (0, 60, 600, 720):
        line(x(t), 270, x(t), 1075, style="--")
    for start, stop, fill in [(-60, 0, "#FFFCF6"), (600, 720, "#F5FBF9")]:
        ax.add_patch(Rectangle((x(start), 270), x(stop) - x(start), 805,
                               facecolor=fill, edgecolor="none", zorder=-1))

    row(300, "End-to-end latency", "Whole-run min, mean, max and p99")
    span(0, 720, 300, purple, "#F0EAF8", "Evaluation messages completed by drain end; before commit acknowledgment")
    row(351, "Unfinished messages and SLO violations", "Acknowledged evaluation messages; 1-second SLO")
    line(x(0), 351, x(720), 351, red, "--", 2)
    ax.plot(x(720), 351, marker="D", color=red, markersize=6)
    text((x(0) + x(720)) / 2, 339, "Final classification at 720 s", 14, red, align="center")
    row(402, "Throughput", "Distinct first completions / measurement duration")
    span(0, 600, 402, blue, "#E9F1FB", "Run throughput: completions during evaluation / 600 s")
    row(453, "Consumer CPU and memory", "Requested cost and actual use")
    span(0, 720, 453, teal, "#E0F3EE", "Whole-run accounting: evaluation + drain (12 min); time-series plots show evaluation")
    row(504, "Backlog, lag and skew", "Original valid observations; interval means use equal weights")
    span(0, 600, 504, blue, "#E9F1FB", "Evaluation observations every 2 s")
    row(555, "Message age", "Messages awaiting completion at observation time")
    line(x(0), 555, x(720), 555, red, "--")

    # These positions illustrate reporting boundaries; they are not trial results.
    active, recovered, window = 105, 300, 30
    for observation in (60, active + window, recovered + window, 600, 720):
        ax.plot(x(observation), 555, marker="D", color=red, markersize=5)
    text(x(180), 541, "Endpoint observations; final unfinished-message ages at 720 s", 14, red)

    row(621, "Before action", "Interval throughput / p99 and workload observations")
    span(30, 60, 621, orange, "#FFF1D8")
    text(x(60) + 18, 621, "30–60 s; original signal samples at 30, 32, …, 58 s", 14, orange)
    row(672, "Active–end", "The full interval from activation to evaluation end")
    span(active, 600, 672, purple, "#F0EAF8", r"$t_A$ to 600 s; may include recovery")
    arrow(x(60), 672, x(active) - 7, 672, purple, "--")
    row(723, "First valid window after activation", "A proposed comparison window, distinct from Active–end")
    span(active, active + window, 723, purple, "#F0EAF8", dashed=True)
    text(x(active + window) + 18, 723, r"First complete valid window after $t_A$; position illustrative*", 14, purple)
    row(774, "Post-recovery window", "A proposed comparison when sufficient evaluation time remains")
    span(recovered, recovered + window, 774, teal, "#E0F3EE", dashed=True)
    text(x(recovered) - 18, 774, r"After confirmed recovery $t_R$*", 14, teal, align="right")
    text(x(recovered + window) + 18, 774, "Not necessarily the final window", 13, teal)
    row(825, "Eval. end", "Final 30 seconds of continuing input")
    span(570, 600, 825, teal, "#E0F3EE")
    text(x(570) - 18, 825, "570–600 s; original signal samples at 570, 572, …, 598 s", 14, teal, align="right")

    row(891, "Rolling mean backlog / skew / persistence", "15 consecutive valid observations; 28-second span")
    span(0, 28, 891, blue, "#CBDDF4")
    arrow(x(28) + 8, 891, x(600), 891, blue, "--")
    text(x(60) + 25, 879, "First: 0, 2, …, 28 s → next: 2, 4, …, 30 s; updates every 2 s", 14, blue)
    row(942, "Rolling mean backlog growth", "Also processing-backlog growth; 15 growth values")
    span(0, 30, 942, purple, "#DCD0EF")
    arrow(x(30) + 8, 942, x(600), 942, purple, "--")
    text(x(60) + 25, 930, "16 observations span 30 s; first: 0, 2, …, 30 s → next: 2, 4, …, 32 s", 14, purple)
    row(993, "Partition / consumer arrival rate", "Pre-action producer acknowledgment counters")
    span(28, 58, 993, orange, "#FFF1D8")
    text(x(60) + 18, 993, "Nominal endpoints: 28 and 58 s; use actual elapsed scrape time", 14, orange)
    row(1044, "Recovery assessment", "Action request → confirmed low-backlog recovery")
    arrow(x(60), 1044, x(600), 1044, teal, "--")
    text(x(60) + 24, 1031, "Q ≤ 100 offsets for 30 s (16 consecutive valid observations) before evaluation ends", 14, teal)

    line(45, 1090, 2140, 1090)
    notes = [
        "* Activation, recovery and subsequent window positions are illustrative. Use recorded times and complete valid windows; activation is not recovery.",
        "Keep 3 uses 60–600 s for Active–end. Request-to-activation time need not be a processing pause. Dashed boxes show proposed comparison windows.",
        "Throughput includes warm-up messages completed during evaluation. Time-series throughput and p99 use consecutive, nonoverlapping 30-second intervals.",
        "Window p99 uses evaluation messages completed in its interval and their full end-to-end latencies; it is not a mean of other p99 values.",
        "Message age is observation time minus creation time for incomplete work. Pre-action backlog may include warm-up work; final unfinished counts use the evaluation cohort.",
        "Capacity uses separate eligible 60-second calibration intervals. Invalid data, excessive gaps and ownership changes restart affected rolling windows; missing is not zero.",
    ]
    for i, note in enumerate(notes):
        text(45, 1117 + 27 * i, note, 12.5, muted)
    for extension in ("png", "svg"):
        path = output_dir / ("experiment-metric-ruler." + extension)
        fig.savefig(path, dpi=150, facecolor="white")
        if extension == "svg":
            path.write_text("\n".join(line.rstrip() for line in path.read_text().splitlines()) + "\n")
        print(path)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    draw(parser.parse_args().output_dir)
