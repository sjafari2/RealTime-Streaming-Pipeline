"""Plot partition input and the consumer demand implied by observed ownership."""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
import os
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/matched-metrics-mpl')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np

ARMS = [('keep3', 'Keep 3'), ('redistribute3', 'Redistribute 3'),
        ('scale_redistribute6', 'Targeted scale 6'), ('kafka_scale6', 'Kafka scale 6')]
CONDITIONS = [('hot12', '12 high-traffic partitions'), ('hot4', '4 high-traffic partitions')]
BLUE, ORANGE = '#5A8EB6', '#D99B5C'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def consumer_rates(rates, owners):
    assert set(owners) == {str(i) for i in range(60)}
    assert set(rates) == {f'0/{i}' for i in range(60)}
    result = defaultdict(float)
    for part, owner in owners.items():
        result[owner] += rates['0/' + part]
    assert abs(sum(result.values()) - sum(rates.values())) < 1e-8
    return dict(result)


def finish(fig, output):
    fig.savefig(str(output) + '.png', dpi=190, facecolor='white')
    fig.savefig(str(output) + '.svg', facecolor='white')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--plot-data', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    comparison = json.loads(args.comparison.read_text())
    saved = json.loads(gzip.decompress(args.plot_data.read_bytes()))
    assert saved['comparison_sha256'] == digest(args.comparison)
    raw = {r['run_id']: r for r in comparison['runs']}
    records = []
    for row in saved['rows']:
        source = raw[row['run_id']]
        records.append(dict(run_id=row['run_id'], condition=row['condition'], arm=row['arm'],
            run=row['run'], partition_rates=row['partition_rates'],
            high_traffic_partitions=[int(p) for p in source['config']['SKEW_PARTITION'].split(',')],
            initial=consumer_rates(row['partition_rates'], source['initial_ownership']),
            final=consumer_rates(row['partition_rates'], source['final_observed_ownership']),
            initial_ownership=source['initial_ownership'], final_ownership=source['final_observed_ownership'],
            producer_intervals=row['arrival_evidence']))
    assert len(records) == 16
    groups = {}
    for condition, _ in CONDITIONS:
        for arm, _ in ARMS:
            pair = sorted((r for r in records if r['condition'] == condition and r['arm'] == arm), key=lambda r:r['run'])
            assert [r['run'] for r in pair] == [1, 2]
            groups[condition, arm] = pair
    args.output.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({'font.size': 11, 'axes.spines.top': False, 'axes.spines.right': False,
                         'svg.fonttype': 'none'})
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.2), sharey=True)
    for ax, (condition, title) in zip(axes, CONDITIONS):
        pair = groups[condition, 'keep3']
        values = np.array([[r['partition_rates'][f'0/{p}'] for p in range(60)] for r in pair])
        mean = values.mean(axis=0)
        assert pair[0]['high_traffic_partitions'] == pair[1]['high_traffic_partitions']
        colors = [ORANGE if p in pair[0]['high_traffic_partitions'] else BLUE for p in range(60)]
        ax.bar(range(60), mean, width=.83, color=colors)
        ax.errorbar(range(60), mean, yerr=[mean-values.min(axis=0), values.max(axis=0)-mean],
                    fmt='none', ecolor='#4E4E4E', elinewidth=.7, capsize=1.4)
        ax.set(title=title, xlabel='Partition index', xlim=(-1, 60), ylim=(0, 140),
               xticks=[0, 10, 20, 30, 40, 50, 59])
        ax.set_axisbelow(True)
        ax.grid(axis='y', alpha=.17)
    axes[0].set_ylabel('Arrival (msg/s)')
    fig.suptitle('Partition arrival rates before action', fontsize=16, weight='bold', y=.99)
    fig.legend([Patch(color=ORANGE), Patch(color=BLUE)], ['High-traffic partitions', 'Lower-traffic partitions'],
               loc='upper center', bbox_to_anchor=(.5, .90), ncol=2, frameon=False)
    fig.subplots_adjust(top=.71, bottom=.15, left=.07, right=.99, wspace=.12)
    finish(fig, args.output/'partition-arrival-bars')

    fig, axes = plt.subplots(4, 2, figsize=(12.5, 11), sharey=True)
    summaries = []
    for i, (arm, label) in enumerate(ARMS):
        for j, (condition, title) in enumerate(CONDITIONS):
            ax = axes[i, j]
            pair = groups[condition, arm]
            for state, shift, color in [('initial', -.19, BLUE), ('final', .19, ORANGE)]:
                active = sorted(set(pair[0][state]) | set(pair[1][state]))
                assert set(pair[0][state]) == set(pair[1][state])
                x = [int(p.rsplit('-',1)[-1]) for p in active]
                values = np.array([[r[state][p] for p in active] for r in pair])
                mean = values.mean(axis=0)
                ax.bar(np.array(x)+shift, mean, .36, color=color,
                       yerr=[mean-values.min(axis=0),values.max(axis=0)-mean],
                       error_kw=dict(elinewidth=1, capsize=3, ecolor='#444444'))
                summaries.append(dict(condition=condition, arm=arm, ownership=state,
                    consumers={p:dict(mean=float(mean[k]), run_values=values[:,k].tolist()) for k,p in enumerate(active)}))
            final_count = len(pair[0]['final'])
            for c in range(3, 6):
                if final_count == 3:
                    ax.text(c, 20, 'Not\nactive', ha='center', va='bottom', fontsize=9, color='#777777')
            if i == 0:
                ax.set_title(title, fontsize=14, pad=12)
            ax.text(.97, .91, label, transform=ax.transAxes, ha='right', va='top', fontsize=11,
                    bbox=dict(facecolor='white', edgecolor='none', alpha=.9, pad=2))
            ax.set(xticks=range(6), xlim=(-.65, 5.65), ylim=(0, 560), yticks=[0,100,200,300,400,500])
            if j == 0:
                ax.set_ylabel('Arrival (msg/s)')
            if i == 3:
                ax.set_xlabel('Consumer index')
            ax.set_axisbelow(True)
            ax.grid(axis='y', alpha=.17)
    fig.suptitle('Consumer arrival rates under initial and final ownership', fontsize=16, weight='bold', y=.99)
    fig.legend([Patch(color=BLUE), Patch(color=ORANGE)], ['Initial ownership', 'Final ownership'],
               loc='upper center', bbox_to_anchor=(.5, .96), ncol=2, frameon=False)
    fig.text(.5, .913, 'Same measured partition rates for both maps', ha='center', color='#555555', fontsize=11)
    fig.subplots_adjust(top=.86, bottom=.06, left=.07, right=.99, hspace=.26, wspace=.12)
    finish(fig, args.output/'consumer-arrival-ownership')
    evidence = dict(comparison_sha256=digest(args.comparison), plot_data_sha256=digest(args.plot_data),
        script_sha256=digest(Path(__file__)), records=records, summaries=summaries,
        method='For each run, sum measured pre-action partition rates over that consumer\'s initial or final observed partition set. Then average the two run-level consumer rates. Whiskers show their observed range.',
        distinction='Final-map rates are calculated using pre-action input measurements. They are not measurements of post-action traffic over time, and exclude stored backlog.',
        partition_figure='The two Keep 3 repetitions provide the partition-rate bars for each workload. All four responses remain available in the source records.',
        missing='Consumers absent from the initial or final map are not active. Their capacity is not inferred from arrival rates.')
    (args.output/'arrival-comparison.json').write_text(json.dumps(evidence, indent=2, allow_nan=False)+'\n')
    print('Created', args.output, flush=True)


if __name__ == '__main__':
    main()
