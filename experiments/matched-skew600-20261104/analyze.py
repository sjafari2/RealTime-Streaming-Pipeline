"""Build category-separated mean plots after a complete matched comparison."""
import argparse
import copy
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator, StrMethodFormatter

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'experiments/twelve-hot600-20261104'))
import plot_repeat_means as mean_plots
import compare_partition_means as comparison_means


def read(path):
    return json.loads(path.read_text())


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def project_campaign(campaign, count):
    """Project a completed 16-trial series without changing individual run evidence."""
    if campaign.get('status') != 'complete' or campaign.get('restoration') != 'verified' or len(campaign.get('runs', [])) != 16:
        raise ValueError('A completed and restored 16-trial series is required')
    state = copy.deepcopy(campaign)
    prefix = f'hot{count}__'
    state['runs'] = [dict(r, arm=r['arm'].split('__', 1)[1]) for r in state['runs'] if r['arm'].startswith(prefix)]
    if sorted((r['arm'], r['run_number']) for r in state['runs']) != sorted((a, n) for a in mean_plots.ARMS for n in (1, 2)):
        raise ValueError('Each workload needs exactly two runs per response')
    state['hot_partitions'] = state['hot_partitions'][:count]
    p = state['cost_protocol']
    p.update(performance_trials=8, hot_partition_count=count,
        conditions={a.split('__', 1)[1]: text.replace('12 hot partitions', f'{count} hot partitions')
                    for a, text in p['conditions'].items() if a.startswith(prefix)},
        order=[(a.split('__', 1)[1], n, s) for a, n, s in p['order'] if a.startswith(prefix)])
    return state


def draw(rows, panels, title, path, placement_caption='Consumer, producer and broker placement matched. Shared-node conditions can still vary.'):
    fig, axes = plt.subplots(len(panels), 2, figsize=(13, 2.75 * len(panels) + 1.8), sharex=True, sharey='row', squeeze=False)
    for column, block in enumerate(('twelve', 'four')):
        for i, (metric, label, step) in enumerate(panels):
            ax = axes[i, column]
            for arm, color in zip(mean_plots.ARMS, mean_plots.COLORS):
                pair = sorted([r for r in rows if r['block'] == block and r['arm'] == arm], key=lambda r: r['run'])
                grid, mean, low, high = mean_plots.pair_mean(pair, metric, step)
                ax.fill_between(grid / 60, low, high, alpha=.13, color=color, linewidth=0)
                ax.plot(grid / 60, mean, color=color, lw=1.7)
                if arm != 'keep3':
                    for r in pair:
                        ax.axvline(r['activation']['after_evaluation_seconds'] / 60, color=color, ls=':', lw=.8, alpha=.7)
            ax.axvline(1, color='#555555', ls=':', lw=1)
            if metric == 'growth':
                ax.axhline(0, color='#777777', lw=.7)
            elif metric == 'throughput':
                ax.axhline(600, color='#777777', lw=.8, ls='--')
            ax.set(xlim=(0, 10), ylabel=label)
            ax.tick_params(labelbottom=True, labelleft=True)
            ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
            ax.yaxis.set_major_formatter(StrMethodFormatter('{x:g}' if metric in ('cpu', 'skew', 'latency', 'p99') else '{x:,.0f}'))
            ax.grid(alpha=.16)
            if i == 0:
                ax.set_title(('12' if block == 'twelve' else '4') + ' high-traffic partitions', fontweight='bold', pad=12)
            if i == len(panels) - 1:
                ax.set_xlabel('Evaluation time (min)')
    for i, (metric, _, _) in enumerate(panels):
        if metric != 'growth':
            axes[i, 0].set_ylim(bottom=0)
    handles = [Line2D([0], [0], color=c, lw=2, label=l) for c, l in zip(mean_plots.COLORS, mean_plots.LABELS)]
    fig.suptitle(title + '\n600 messages/s, mean of two runs per response', fontsize=15, y=.985)
    fig.legend(handles=handles, loc='upper center', ncol=4, bbox_to_anchor=(.5, .925 if len(panels) > 2 else .88), frameon=False, fontsize=10)
    note = ('Lines: two-run means. Bands: observed ranges. Missing values remain gaps.\n'
            'Dotted lines: gray = action request at minute 1, colors = confirmed activation in each run.\n'
            + placement_caption)
    if panels[0][0] == 'backlog':
        note += '\n30-second windows. Latency: completed evaluation messages. Throughput includes completed warm-up work. Dashed line: 600 msg/s. Drain excluded.'
    elif panels[0][0] == 'growth':
        note += '\nGrowth uses 15 two-second intervals. Skew uses 15 observations. Ownership changes restart rolling windows.'
    else:
        note += '\nCPU and RSS memory are summed across started consumer processes. Evaluation only. Resource requests are not plotted.'
    fig.text(.5, .015, note, ha='center', va='bottom', fontsize=8.8)
    fig.subplots_adjust(left=.08, right=.97, top=.85 if len(panels) > 2 else .76,
                        bottom=.12 if len(panels) > 2 else .21, hspace=.23, wspace=.21)
    for extension in ('png', 'pdf'):
        fig.savefig(path.with_suffix('.' + extension), dpi=180, facecolor='white')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('series_status', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--wait', action='store_true', help='Wait for this one series, at most nine hours')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    status_file = args.output / 'analysis-status.json'
    deadline = time.monotonic() + 9 * 3600
    while True:
        series = read(args.series_status)
        if series['status'] in ('failed', 'stopped_for_review'):
            save(status_file, dict(status='not_run', reason=series.get('error', series['status'])))
            return
        if series['status'] == 'complete' and series.get('restoration') == 'verified':
            break
        if not args.wait or time.monotonic() > deadline:
            raise RuntimeError('Series has not completed and restored its deployment')
        time.sleep(30)
    save(status_file, dict(status='analyzing', started_epoch=time.time()))
    campaign_dir = Path(series['campaign_directory'])
    campaign = read(campaign_dir / 'campaign-status.json')
    spec = importlib.util.spec_from_file_location('matched_summary', ROOT / 'experiments/four-condition-20260929/summarize.py')
    summary = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(summary)
    resources_files = sorted(campaign_dir.rglob('resource-observations.jsonl'))
    resources = sorted([json.loads(line) for p in resources_files for line in p.read_text().splitlines()], key=lambda x: x['timestamp'])
    rows, comparisons = [], {}
    for count, block in ((12, 'twelve'), (4, 'four')):
        state = project_campaign(campaign, count)
        arms, rate = summary.campaign_design(state)
        outcomes = []
        for trial in sorted(state['runs'], key=lambda r: (arms.index(r['arm']), r['run_number'])):
            directory = Path(trial['directory'])
            checks = [json.loads(line) for line in (directory / 'matched-placement-checks.jsonl').read_text().splitlines()]
            if not checks or any(c['issues'] for c in checks):
                raise ValueError('Missing or failed matched-placement evidence: ' + trial['run_id'])
            result, _, _ = summary.load(trial, resources, state['reference'], state['hot_partitions'], 15, rate, 1000, [500, 1000])
            result['matched_placement_sha256'] = hashlib.sha256((directory / 'matched-placement-checks.jsonl').read_bytes()).hexdigest()
            outcomes.append(result)
            print('Analyzed', count, trial['arm'], trial['run_number'], flush=True)
        comparison = dict(campaign_revision=state['code_commit'], cost_protocol=state['cost_protocol'],
            hot_partitions=state['hot_partitions'], runs=outcomes,
            analysis_environment=dict(revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
                script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),
            resource_observer_sha256={str(p.relative_to(campaign_dir)): hashlib.sha256(p.read_bytes()).hexdigest() for p in resources_files})
        path = args.output / block / 'comparison.json'
        save(path, comparison)
        block_rows = mean_plots.reconstruct(path, path.parent)
        for row in block_rows:
            row['block'] = block
        rows.extend(block_rows)
        comparisons[block] = comparison
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11,
                         'axes.spines.top': False, 'axes.spines.right': False})
    draw(rows, [('backlog', 'Total backlog (offsets)', 2), ('throughput', 'Throughput (msg/s)', 30),
                ('latency', 'Mean latency (s)', 30), ('p99', 'p99 latency (s)', 30)],
         'Performance evaluation metrics', args.output / 'performance-12-vs-4')
    draw(rows, [('growth', 'Backlog growth (offsets/s)', 2), ('skew', 'Lag skew ratio', 2)],
         'Runtime control signals', args.output / 'control-signals-12-vs-4')
    draw(rows, [('cpu', 'CPU use (cores)', 2), ('memory', 'Memory use (MiB)', 2)],
         'Resource use', args.output / 'resource-use-12-vs-4')
    save(args.output / 'mean-results.json', dict(summaries=comparison_means.summarize(rows, comparisons),
        interpretation='Equal-weight means of two independent runs. Mean run p99 is not a pooled percentile. Whole-run latency follows evaluation messages through drain. Time-series plots exclude drain.'))
    with gzip.open(args.output / 'comparison-plot-data.json.gz', 'wt') as stream:
        json.dump(dict(rows=rows), stream, allow_nan=False)
    save(status_file, dict(status='complete', finished_epoch=time.time(), trials=len(rows), figures=3))
    print('[PLOTS COMPLETE]', args.output, flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        if len(sys.argv) >= 3 and not sys.argv[2].startswith('-'):
            save(Path(sys.argv[2]) / 'analysis-status.json', dict(status='failed', error=str(exc), finished_epoch=time.time()))
        raise
