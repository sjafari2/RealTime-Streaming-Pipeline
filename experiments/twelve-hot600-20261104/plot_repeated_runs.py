"""Compare repeated 600 msg/s runs without pooling deployment blocks."""
import argparse
import hashlib
import gzip
import json
import os
from pathlib import Path
import statistics

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/twelve-hot600-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
WORK = Path(__file__).resolve().parent
ARMS = ['keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6']
LABELS = ['Keep 3', 'Redistribute 3', 'Targeted scale 6', 'Kafka scale 6']
COLORS = ['#2563a6', '#dc862d', '#21875e', '#9a4eaa']


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(gzip.decompress(path.read_bytes()).decode() if path.suffix == '.gz' else path.read_text())


def add_backlog(row):
    directory = ROOT / 'results' / row['run_id']
    path = directory / 'lag-summary.json'
    assert digest(path) == row['source_hashes']['lag-summary.json']
    manifest = read(directory / 'manifest.json')
    start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
    row['total_backlog'] = []
    row['growth'] = []
    previous = None
    for sample in read(path)['snapshots']:
        if start <= sample['timestamp'] <= end:
            changed = previous is not None and sample.get('owners') != previous.get('owners')
            row['total_backlog'].append([sample['timestamp'] - start,
                                        sample.get('total_lag') if sample['valid'] and not changed else None])
            row['growth'].append([sample['timestamp'] - start,
                                 sample.get('window_growth_offsets_per_second') if sample['valid'] and not changed else None])
            previous = sample
    return row


def aligned(series, step):
    """Align scrape jitter only. Never interpolate unavailable observations."""
    result = np.full(301 if step == 2 else 20, np.nan)
    breaks = set()
    for t, value in series:
        nominal = round(t / step) * step
        if abs(nominal - t) > .5:
            continue
        index = round(nominal / step) - (1 if step == 30 else 0)
        if 0 <= index < len(result):
            if value is None:
                breaks.add(index)
            result[index] = value if value is not None and index not in breaks else np.nan
    return result


def draw(rows, groups, metrics, output, title):
    fig, axes = plt.subplots(len(metrics), len(groups), figsize=(4.7*len(groups), 3.0*len(metrics)),
                             sharex=True, sharey='row', squeeze=False)
    plt.rcParams.update({'font.size': 10})
    for col, (campaign, condition, heading) in enumerate(groups):
        subset = [r for r in rows if r['campaign'] == campaign and r['condition'] == condition]
        for j, (metric, ylabel, step) in enumerate(metrics):
            ax = axes[j, col]
            for arm, label, color in zip(ARMS, LABELS, COLORS):
                pair = sorted((r for r in subset if r['arm'] == arm), key=lambda r:r['run'])
                assert len(pair) == 2, (campaign, condition, arm, len(pair))
                values = np.array([aligned(r[metric], step) for r in pair])
                t = np.arange(0, 602, 2) if step == 2 else np.arange(30, 601, 30)
                valid = np.isfinite(values).all(axis=0)
                mean = np.where(valid, values.mean(axis=0), np.nan)
                low, high = np.where(valid, values.min(axis=0), np.nan), np.where(valid, values.max(axis=0), np.nan)
                ax.fill_between(t/60, low, high, color=color, alpha=.13, linewidth=0)
                ax.plot(t/60, mean, color=color, lw=1.8, label=label)
                if arm != 'keep3':
                    for r in pair:
                        active = r['activation']['after_evaluation_seconds']
                        if active is not None:
                            ax.axvline(active/60, color=color, ls=':', lw=.8, alpha=.65)
            ax.axvline(1, color='#555555', ls=':', lw=1)
            ax.set_xlim(0, 10)
            ax.grid(alpha=.16)
            if col == 0:
                ax.set_ylabel(ylabel, fontsize=10)
            if j == 0:
                ax.set_title(heading, fontsize=12, pad=10)
            if j == len(metrics)-1:
                ax.set_xlabel('Evaluation time (min)')
            ax.ticklabel_format(axis='y', style='plain')
    for j, (metric, _, _) in enumerate(metrics):
        maximum = max(v for row in rows for _,v in row[metric] if v is not None)
        axes[j,0].set_ylim(0,maximum*1.05)
    handles, labels = axes[0,0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', ncol=4, frameon=False, bbox_to_anchor=(.5,.955))
    fig.suptitle(title, fontsize=16, y=.995)
    fig.text(.5, .018,
             'Lines: mean of 2 runs. Bands: observed range, not confidence intervals. Gaps: missing data or ownership transitions.\n'
             'Dotted lines: gray = request at 60 s, colors = each run’s confirmed activation. Deployment blocks remain separate.',
             ha='center', fontsize=9)
    fig.tight_layout(rect=(0,.065,1,.92))
    for suffix in ('png','pdf'):
        fig.savefig(output.with_suffix('.'+suffix), dpi=180)
    plt.close(fig)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--cache',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--new-cache',type=Path)
    args=parser.parse_args()
    output=args.output
    output.mkdir(parents=True,exist_ok=True)
    rows=[r if 'total_backlog' in r else add_backlog(r) for r in read(args.cache)['rows'] if r['rate']==600]
    groups=[('four600',None,'4 high-traffic partitions'),
            ('starting','smaller','12 high-traffic partitions\nSmaller starting backlog'),
            ('starting','larger','12 high-traffic partitions\nLarger starting backlog')]
    if args.new_cache:
        rows.extend(r if 'total_backlog' in r else add_backlog(r) for r in read(args.new_cache)['rows'])
    if any(r['campaign']=='twelve600' for r in rows):
        groups.insert(0,('twelve600',None,'12 high-traffic partitions\nStandard preparation'))
    assert len(rows)==len(set(r['run_id'] for r in rows))
    definitions=dict(total_backlog='Sum of log end minus consumer position. Ownership transitions break the plotted line.',
        growth='Rolling mean growth of total consumer-position backlog, using 15 consecutive valid changes.',
        throughput='Distinct completions in nonoverlapping 30-second evaluation intervals. Includes warm-up messages completed during evaluation.',
        window_p99='Nearest-rank p99 of original creation-to-completion latency for evaluation messages completed within the interval.',
        cpu='Observed CPU cores across all started consumers. Missing components remain unavailable.',
        memory='Observed RSS MiB across all started consumers. Missing components remain unavailable.',
        averages='Equal-weight means of two runs at common valid times. Bands show the two-run range.',
        scope='Time-series data exclude drain. Whole-run p99 and unfinished percentages follow evaluation messages through drain.',
        comparison='Deployment blocks are kept separate. Differences across campaigns cannot be attributed to skew or starting backlog alone.')
    (output/'repeat-plot-data.json.gz').write_bytes(gzip.compress(json.dumps(dict(rows=rows,definitions=definitions),allow_nan=False).encode(),mtime=0))
    draw(rows,groups,[('total_backlog','Total backlog (offsets)',2),
                      ('window_p99','Window p99\nend-to-end latency (s)',30),
                      ('throughput','Throughput (messages/s)',30)],
         output/'mean-performance-600','Performance evaluation at 600 messages/s')
    draw(rows,groups,[('cpu','Actual consumer CPU use\n(cores)',2),
                      ('memory','Actual consumer RSS memory\n(MiB)',2)],
         output/'mean-resource-use-600','Actual resource use at 600 messages/s')
    summaries=[]
    for campaign,condition,heading in groups:
        for arm in ARMS:
            pair=[r for r in rows if r['campaign']==campaign and r['condition']==condition and r['arm']==arm]
            values={}
            for metric in ('whole_run_p99_seconds','unfinished_percent'):
                v=[r[metric] for r in pair]
                values[metric]={'individual':v,'mean':statistics.mean(v),'sample_std':statistics.stdev(v)}
            summaries.append(dict(campaign=campaign,condition=condition,arm=arm,
                                  run_ids=[r['run_id'] for r in pair],metrics=values))
    (output/'mean-results.json').write_text(json.dumps(dict(
        notes=['Mean run-level p99 is not a pooled-message percentile.',
               'Window p99 means average replicate window percentiles.',
               'Only times with valid values in both runs contribute to time-series means.',
               'Cross-campaign differences do not isolate traffic concentration or backlog size from deployment effects.',
               'Starting-backlog preparation changes both backlog amount and message age.'],
        summaries=summaries),indent=2)+'\n')
    print('Created comparison figures for',len(rows),'completed trials in',output)


if __name__=='__main__':
    main()
