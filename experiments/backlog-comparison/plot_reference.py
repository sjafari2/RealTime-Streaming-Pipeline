"""Plot a saved balanced-reference record without loading raw message logs."""
import argparse
import json
import os
from pathlib import Path
import tempfile

os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir()) / 'stream-plot-cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('record', type=Path)
args = parser.parse_args()
rows = json.loads((args.record / 'backlog-series.json').read_text())
summary = json.loads((args.record / 'summary.json').read_text())
duration = summary['phases_seconds']['evaluation'] / 60
fig, axes = plt.subplots(2, 1, figsize=(10, 6.3), layout='constrained')
for ax, lower, title in zip(axes, (0, 5), ('Full evaluation', f'Detail: minutes 5–{duration:g}')):
    selected = [x for x in rows if x['seconds'] >= lower*60]
    for metric, color, label in [('processing_backlog', '#17619A', 'Processing backlog'),
                                  ('total_lag', '#D57425', 'Consumer lag')]:
        ax.plot([x['seconds']/60 for x in selected],
                [x[metric] if x['valid'] else float('nan') for x in selected],
                color=color, linewidth=1.1, label=label)
    ax.set(xlim=(lower, duration), ylim=(0, None), ylabel='Offsets', title=title)
    ax.grid(alpha=.2)
axes[0].legend(frameon=False)
axes[1].set_xlabel('Minutes after evaluation starts (warm-up and drain excluded)')
fig.suptitle(f"Balanced input: {summary['aggregate_input_rate']} messages/s total, {summary['consumers']} consumers")
for extension in ('png', 'pdf'):
    fig.savefig(args.record / ('balanced600-backlog.' + extension), dpi=180)
plt.close(fig)
