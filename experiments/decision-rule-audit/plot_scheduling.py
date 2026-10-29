"""Plot observed service order and allocation from the saved scheduling analysis."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    report = json.loads((args.directory / 'results.json').read_text())
    run = next(run for run in report['runs'] if run['run_id'] == 'run-20261027-004457')
    consumer = next(p for p in run['processes'] if p['pod'] == 'consumer-sts-0')
    hot = set(run['hot_partitions'])
    trace = consumer['first_200_processing_starts'][:120]
    partitions = consumer['partitions']
    colors = ['#bd5c36' if p['partition'] in hot else '#3277a5' for p in partitions]
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8), layout='constrained')
    ax = axes[0, 0]
    ax.scatter(range(1, len(trace) + 1), [r['partition'] for r in trace],
               c=['#bd5c36' if r['partition'] in hot else '#3277a5' for r in trace], s=18)
    ax.set(xlabel='Processing order (first 120 starts after second 30)', ylabel='Partition index',
           title='A. Frequent switching among partitions', yticks=range(0, 60, 6))
    ax = axes[0, 1]
    ax.bar(range(len(partitions)), [p['observed_start_rate'] for p in partitions], color=colors)
    ax.axhline(consumer['observed_start_rate'] / len(partitions), color='#525252', ls='--', lw=1.3,
               label='Equal share of observed total (reference)')
    ax.set(xlabel='Partition index', ylabel='Processing starts per second',
           title='B. Unequal service per partition', xticks=range(len(partitions)),
           xticklabels=[p['partition'] for p in partitions])
    ax.tick_params(axis='x', labelsize=8)
    ax.legend(fontsize=8, loc='upper right')
    ax = axes[1, 0]
    ax.bar(range(len(partitions)), [p['minimum_observed_backlog'] for p in partitions], color=colors)
    ax.set_ylim(0, 1.3 * max(p['minimum_observed_backlog'] for p in partitions))
    ax.set(xlabel='Partition index', ylabel='Minimum sampled processing backlog (offsets)',
           title='C. Every assigned partition had queued work', xticks=range(len(partitions)),
           xticklabels=[p['partition'] for p in partitions])
    ax.tick_params(axis='x', labelsize=8)
    ax.text(.98, .95, '15 valid samples per partition\nPositive at each sample; local fetch queues unobserved',
            transform=ax.transAxes, ha='right', va='top', fontsize=8)
    ax = axes[1, 1]
    for index, observed in enumerate(report['runs']):
        process = next(p for p in observed['processes'] if p['pod'] == 'consumer-sts-0')
        hot_ids = set(observed['hot_partitions'])
        rates = [[p['observed_start_rate'] for p in process['partitions'] if (p['partition'] in hot_ids) == is_hot]
                 for is_hot in (True, False)]
        marker = 'o' if observed['backlog_condition'] == 'smaller' else '^'
        for xpos, values, color in zip([0, 1], rates, ['#bd5c36', '#3277a5']):
            ax.scatter(xpos + (index - 7.5) * .015, sum(values) / len(values),
                       color=color, marker=marker, s=35, alpha=.8)
    ax.set(xlim=(-.4, 1.4), xticks=[0, 1], xticklabels=['High-input partition', 'Other assigned partition'],
           ylabel='Mean processing starts/s per partition',
           title='D. Same pattern across the 16 pre-action windows')
    ax.legend(handles=[Line2D([], [], marker='o', color='#555555', ls='', label='Smaller starting backlog'),
                       Line2D([], [], marker='^', color='#555555', ls='', label='Larger starting backlog')], fontsize=8)
    for ax in axes.flat:
        ax.grid(axis='y', alpha=.18)
        ax.set_axisbelow(True)
        ax.spines[['top', 'right']].set_visible(False)
    fig.suptitle('Observed consumer scheduling before mitigation (evaluation seconds 30–60)\n'
                 'Panels A–C: smaller backlog, Keep 3, Run 1, consumer-sts-0', fontsize=13)
    fig.savefig(args.directory / 'observed-scheduling.png', dpi=180)
    plt.close(fig)


if __name__ == '__main__':
    main()
