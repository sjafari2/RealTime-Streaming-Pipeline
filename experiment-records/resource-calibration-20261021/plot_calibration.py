"""Draw a resource calibration from saved evidence and validated monitoring."""
import argparse
import json
import os
from pathlib import Path
import sys

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/resource-calibration-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('run', type=Path)
parser.add_argument('output', type=Path)
parser.add_argument('--code', type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.code / 'python-scripts'))
from comparison_series import resource_line, throughput_bins

read = lambda name: json.loads((args.run / name).read_text())
manifest, lag = read('manifest.json'), read('lag-summary.json')
series = read('prometheus.json')['data']['result']
start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
x, backlog, fetch = [], [], []
previous = None
for row in lag['snapshots']:
    if not start <= row['timestamp'] <= end:
        continue
    good = row['valid']
    broken = good and previous and (row['timestamp']-previous['timestamp'] > lag['parameters']['max_gap'] or
                    row.get('owners') != previous.get('owners') or any(row[field][key] < value
                    for field in ('highs', 'positions') for key, value in previous[field].items()))
    if broken:
        x.append(np.nan); backlog.append(np.nan); fetch.append(np.nan)
    x.append((row['timestamp']-start)/60)
    backlog.append(row.get('processing_backlog', np.nan) if good else np.nan)
    fetch.append(row.get('total_lag', np.nan) if good else np.nan)
    previous = row if good else None

plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
fig, axes = plt.subplots(3, 2, figsize=(12, 10), layout='constrained')
axes[0,0].plot(x, backlog, label='Processing backlog', color='#216a89', lw=1.4)
axes[0,0].plot(x, fetch, label='Not yet fetched', color='#b66a2c', lw=1)
axes[0,0].set(title='Total backlog during continuing input', ylabel='Offsets')
t, rates = throughput_bins(args.run, start, end)
axes[0,1].plot(t, rates[0], label='Acknowledged input', color='#216a89')
axes[0,1].plot(t, rates[1], label='Distinct completions', color='#568244')
target_rate = float(manifest['config']['TARGET_RATE'])*int(manifest['config']['PRODUCER_POD_COUNT'])
axes[0,1].axhline(target_rate, color='#777777', lw=.8, ls=':', label=f'Target: {target_rate:g}')
axes[0,1].set(title='Throughput in 10-second bins', ylabel='Messages / second')
colors = ['#216a89', '#b66a2c', '#568244']
for column, role in enumerate(('producer', 'consumer')):
    for pod, color in zip(manifest[role+'_pods'], colors):
        for row_number, metric, divisor in [(1, role+'_cpu_percent', 100), (2, role+'_memory_bytes', 2**20)]:
            times, values = resource_line(series, metric, pod, start, end, divisor)
            axes[row_number,column].plot(times, values, label=pod, color=color, lw=1.2)
    axes[1,column].set(title=role.capitalize()+' process CPU', ylabel='CPU cores')
    axes[2,column].set(title=role.capitalize()+' process memory', ylabel='RSS (MiB)')
for ax in axes.flat:
    ax.set_xlim(0, (end-start)/60)
    ax.set_ylim(bottom=0)
    ax.set_xlabel('Minutes from evaluation start')
    ax.grid(axis='y', alpha=.2)
    ax.legend(fontsize=8, frameon=False)
warmup = (start-manifest['start_epoch'])/60
evaluation = (end-start)/60
drain = (manifest['drain_end_epoch']-end)/60
fig.suptitle(f'Balanced resource calibration: {target_rate:g} messages/s, {len(manifest["consumer_pods"])} consumers\n'
             f'{warmup:g}-minute warm-up + {evaluation:.2f}-minute evaluation + {drain:g}-minute drain', fontsize=15)
fig.supxlabel('Evaluation shown. Missing or discontinuous monitoring remains a gap. CPU and RSS are process metrics;\n'
              'container memory peaks, throttling and restart checks are reported separately.', fontsize=9)
args.output.parent.mkdir(parents=True, exist_ok=True)
for extension in ('png', 'pdf'):
    fig.savefig(args.output.with_suffix('.'+extension), dpi=180, facecolor='white')
print(args.output.with_suffix('.png'))
