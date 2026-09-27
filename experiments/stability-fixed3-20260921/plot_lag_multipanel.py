"""Compare validated balanced stability runs, retaining gaps in each lag trace."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/stability-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter


def load_run(directory):
    read = lambda name: json.loads((directory / name).read_text())
    status, manifest, lag = [read(name) for name in
                             ('runner-status.json', 'manifest.json', 'lag-summary.json')]
    if status.get('status') != 'complete' or status.get('issues'):
        raise ValueError('Run must pass validation: ' + directory.name)
    cfg = manifest['config']
    required = dict(TRAFFIC_MODE='balanced', CONSUMER_POD_COUNT='3',
                    PRODUCER_POD_COUNT='3', NUM_PARTITIONS='60', TOPIC_COUNT='1',
                    APP_CPU_ITERATIONS='2000', APP_DELAY_MS='0', MITIGATION_POLICY='none',
                    WARMUP_SECONDS='60', DRAIN_SECONDS='120')
    if any(cfg.get(k) != v for k, v in required.items()) or lag['evaluation_seconds'] != 1200:
        raise ValueError('Run differs from the comparison design: ' + directory.name)
    if manifest.get('intervention', {}).get('action') != 'none':
        raise ValueError('Unexpected intervention: ' + directory.name)
    rate = float(cfg['TARGET_RATE']) * int(cfg['PRODUCER_POD_COUNT'])
    number = int(cfg['EXP_ID'].rsplit('run', 1)[-1])
    x, y, previous = [], [], None
    for row in lag['snapshots']:
        elapsed = row['timestamp'] - manifest['evaluation_start_epoch']
        if elapsed < -0.001 or elapsed > 1200.001:
            continue
        valid = row.get('valid') and row.get('total_lag') is not None
        if valid and previous is not None:
            dt = row['timestamp'] - previous['timestamp']
            reset = any(row[field].get(k, v) < v for field in ('highs', 'positions')
                        for k, v in previous[field].items())
            if not 0 < dt <= lag['parameters']['max_gap'] or row['owners'] != previous['owners'] or reset:
                x.append(elapsed / 60)
                y.append(math.nan)
        x.append(elapsed / 60)
        y.append(row['total_lag'] if valid else math.nan)
        previous = row if valid else None
    if not any(math.isfinite(v) for v in y):
        raise ValueError('No valid evaluation lag: ' + directory.name)
    hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
              for name in ('manifest.json', 'runner-status.json', 'lag-summary.json')}
    return dict(run_id=directory.name, rate=rate, number=number, x=x, y=y,
                peak=lag['peak_sampled_lag'], evidence_sha256=hashes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('run_directories', type=Path, nargs='+')
    args = parser.parse_args()
    runs = sorted((load_run(p) for p in args.run_directories), key=lambda r: (r['rate'], r['number']))
    identities = {(r['rate'], r['number']) for r in runs}
    if len(identities) != len(runs):
        raise ValueError('Duplicate rate/run number')
    rates = sorted({r['rate'] for r in runs})
    columns = 3
    rows = math.ceil(len(rates) / columns)
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False})
    fig, axes = plt.subplots(rows, columns, figsize=(15, 4.1 * rows + 1), squeeze=False)
    palette = ['#176b93', '#d86a26', '#408b57']
    for ax, rate in zip(axes.flat, rates):
        selected = [r for r in runs if r['rate'] == rate]
        for run in selected:
            ax.plot(run['x'], run['y'], color=palette[(run['number'] - 1) % len(palette)],
                    linewidth=1.1, alpha=.9, label='Run ' + str(run['number']))
        # The low-rate panels share a scale; higher-rate panels need larger ranges.
        peak = max(r['peak'] for r in selected)
        upper = 500 if rate <= 800 else math.ceil(peak * 1.18 / 1000) * 1000
        ax.set(xlim=(0, 20), ylim=(0, upper), xticks=[0, 5, 10, 15, 20])
        ax.set_title(f'{rate:,.0f} messages/s', loc='left', fontsize=13, fontweight='bold', pad=12)
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        ax.grid(axis='y', color='#d9e0e5', linewidth=.7, alpha=.8)
        ax.legend(loc='upper left', frameon=False, fontsize=9)
    for ax in list(axes.flat)[len(rates):]:
        ax.set_visible(False)
    fig.suptitle('Balanced input: lag across all tested rates', x=.07, ha='left',
                 y=.985, fontsize=21, fontweight='bold')
    fig.text(.07, .935, f'{len(runs)} validated runs  |  3 consumers  |  60 partitions  |  no scaling',
             fontsize=12, color='#52606a')
    fig.supylabel('Total lag (offsets)', x=.012, fontsize=12)
    fig.supxlabel('Time since evaluation started (minutes)', y=.09, fontsize=12)
    fig.text(.07, .048, 'Each panel shows 20 minutes of evaluation; 1-minute warm-up and 2-minute drain are not shown.',
             fontsize=10, color='#52606a')
    fig.text(.07, .019, 'The 600–800 panels share a 0–500 scale. Higher-rate panels use different vertical scales. Gaps remain visible.',
             fontsize=10, color='#52606a')
    fig.subplots_adjust(left=.075, right=.98, top=.86, bottom=.17, hspace=.35, wspace=.27)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output.with_suffix('.png'), dpi=200, facecolor='white')
    fig.savefig(args.output.with_suffix('.pdf'), facecolor='white')
    # Record exact input identities and hashes without duplicating raw time series.
    args.output.with_suffix('.json').write_text(json.dumps(
        [{k: v for k, v in r.items() if k not in ('x', 'y')} for r in runs], indent=2) + '\n')
    plt.close(fig)
    print(f'Saved {len(runs)} runs in {len(rates)} panels: {args.output}')


if __name__ == '__main__':
    main()
