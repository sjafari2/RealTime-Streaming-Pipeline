"""Plot total observed consumer CPU and RSS for the four-condition comparison."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir()) / 'four-hot600-resource-mpl'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison', type=Path)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--source-repository', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.source_repository / 'experiments/result-metric-audit-20260930'))
    from build_compact_figures import process_total
    data = json.loads(args.comparison.read_text())
    colors = {'keep3': '#2563a6', 'redistribute3': '#dc862d',
              'scale_redistribute6': '#21875e', 'kafka_scale6': '#9a4eaa'}
    labels = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute 3',
              'scale_redistribute6': 'Targeted Scale 6', 'kafka_scale6': 'Kafka Scale 6'}
    fig, axes = plt.subplots(2, 2, figsize=(10, 6.8), sharex=True, sharey='row')
    saved = {}
    for row in data['runs']:
        directory = args.repository / row['raw_evidence']
        for name in ('manifest.json', 'prometheus.json'):
            assert hashlib.sha256((directory/name).read_bytes()).hexdigest() == row['evidence_hashes'][name]
        manifest = json.loads((directory/'manifest.json').read_text())
        prom = json.loads((directory/'prometheus.json').read_text())['data']['result']
        starts = {}
        for name, expected_hash in row['intervention_cost']['event_file_sha256'].items():
            if not name.startswith('consumer/'):
                continue
            path = directory/name
            assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash
            with path.open() as stream:
                first = json.loads(next(stream))
            assert first['event'] == 'started'
            pod = json.loads((path.parent/'final.json').read_text())['pod']
            assert pod not in starts
            starts[pod] = first['timestamp']
        saved[row['run_id']] = {}
        for i, (metric, divisor) in enumerate([('consumer_cpu_percent', 100), ('consumer_memory_bytes', 2**20)]):
            series = process_total(prom, metric, divisor, starts,
                                   manifest['evaluation_start_epoch'], manifest['producer_end_epoch'])
            saved[row['run_id']][metric] = series
            axes[i, row['run_number']-1].plot(series['minutes'], series['values'],
                                           color=colors[row['arm']], lw=1.3)
    for col in range(2):
        axes[0, col].set_title('Run '+str(col+1))
        axes[1, col].set_xlabel('Minutes since evaluation began')
        for ax in axes[:, col]:
            ax.set_xlim(0, 10); ax.set_ylim(bottom=0)
            ax.axvline(1, color='#666666', ls=':', lw=1)
            ax.grid(alpha=.2)
    axes[0, 0].set_ylabel('Total consumer process CPU (cores)')
    axes[1, 0].set_ylabel('Total consumer RSS (MiB)')
    fig.suptitle('Four high-input partitions at 600 messages/s: observed resources', y=.99)
    fig.legend([Line2D([0],[0],color=colors[k],lw=2) for k in colors], list(labels.values()),
               ncol=4, loc='upper center', bbox_to_anchor=(.53,.955), frameon=False, fontsize=9)
    fig.subplots_adjust(left=.11, right=.98, top=.85, bottom=.16, hspace=.16, wspace=.12)
    fig.text(.53,.025,'Totals include every started consumer process only when all readings are fresh. Missing data remain gaps.\n'
             'Dotted: scheduled action at minute 1. Evaluation only; requested resources are reported separately.',
             ha='center',fontsize=9)
    args.output.mkdir(parents=True,exist_ok=True)
    for extension in ('png','pdf'):
        fig.savefig(args.output/('four-hot600-resources.'+extension),dpi=180)
    plt.close(fig)
    (args.output/'four-hot600-resource-series.json').write_text(json.dumps(saved,indent=2)+'\n')


if __name__ == '__main__':
    main()
