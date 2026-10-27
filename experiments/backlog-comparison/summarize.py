"""Report completed backlog comparisons without filling missing trials or samples."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path[:0] = [str(ROOT / 'python-scripts'), str(ROOT / 'experiments/signal-outcome-20261021')]
os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/stream-plot-cache')
from build_capacity import CounterData, partition_arrivals, stable_lag
from intervention_cost import recovery


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


previous = module('comparison_summary', 'experiments/four-condition-20260929/summarize.py')
completion = module('completion_windows', 'experiments/four-condition-20260929/analyze_action_windows.py')
signal = module('signal_windows', 'experiments/four-hot600-20261021/analyze_signal_windows.py')
ARMS, LABELS = previous.ARMS, previous.LABELS


def arrival_capacity(directory, manifest, lag, left, right, capacities):
    data = CounterData(json.loads((directory / 'prometheus.json').read_text()), manifest)
    try:
        arrivals = partition_arrivals(data, manifest, left, right)
        snapshots = stable_lag(lag['snapshots'], manifest['evaluation_start_epoch'], left, right)
    except ValueError as exc:
        return dict(status='unavailable', reason=str(exc), interval_seconds=[left, right])
    by_consumer = {}
    owners = snapshots[-1]['owners']
    for part, rate in arrivals['partitions'].items():
        topic_index, partition = part.split('/')
        topic = manifest['config']['TOPIC_TITLE'] + '_' + topic_index
        owner = owners[topic + '/' + partition].split('/')[0]
        by_consumer[owner] = by_consumer.get(owner, 0) + rate
    rows = {}
    for pod, rate in by_consumer.items():
        capacity = capacities.get(pod, {}).get('summary', {}).get('effective_rate')
        rows[pod] = dict(arrival_rate=rate, calibrated_capacity=capacity,
            calibrated_margin=capacity-rate if capacity is not None else None,
            calibration_limit='Added consumers have no matching busy-capacity calibration.' if capacity is None
                else 'Balanced busy calibration on the same original pod; not a guaranteed service rate under every assignment.')
    return dict(status='available', interval_seconds=[left, right], measured_partition_rates=arrivals,
                consumers=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--partial', action='store_true')
    args = parser.parse_args()
    state = json.loads((args.campaign / 'campaign-status.json').read_text())
    protocol = state['cost_protocol']
    expected = [tuple(row) for row in protocol['order']]
    observed = [(row['arm'], row['run_number'], row['seed']) for row in state['runs']]
    if observed != expected[:len(observed)] or not observed:
        raise ValueError('Completed records must be a nonempty prefix of the planned order')
    complete = state['status'] == 'complete' and state.get('restoration') == 'verified' and observed == expected
    if not complete and not args.partial:
        raise ValueError('Campaign is incomplete; use --partial only for an explicitly provisional report')
    resources = []
    for path in args.campaign.rglob('resource-observations.jsonl'):
        raw = path.read_text()
        # An active observer can be in the middle of its final append.
        for line in raw[:raw.rfind('\n')+1].splitlines():
            resources.append(json.loads(line))
    resources.sort(key=lambda row: row['timestamp'])
    summaries, plot_data = [], []
    for row in state['runs']:
        if row['validation']['status'] != 'passed':
            raise ValueError('A comparison has not passed measurement validation')
        level, arm = row['arm'].split('__')
        summary, curves, marks = previous.load(dict(row, arm=arm), resources, state['reference'],
            state['hot_partitions'], 15, 600, 1000, [500, 1000], protocol['initial_hot_owner'])
        directory = Path(row['directory'])
        manifest = json.loads((directory / 'manifest.json').read_text())
        lag = json.loads((directory / 'lag-summary.json').read_text())
        start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
        ack, done, hashes = completion.load_messages(directory, manifest, summary['intervention_cost']['event_file_sha256'])
        messages = completion.build_series(ack, done, start, end)
        activation = completion.post_action_boundary(directory, manifest, summary)
        windows = {}
        for name, left, right in [('before', start+30, start+60),
                                 ('activation_to_end', activation['epoch'], end),
                                 ('evaluation_end', end-30, end)]:
            count = 3 if name == 'before' or arm in ('keep3', 'redistribute3') else 6
            prom = json.loads((directory / 'prometheus.json').read_text())['data']['result']
            windows[name] = dict(completion=completion.window_metrics(messages, left, right, 1000),
                signals=signal.signal_window(lag['snapshots'], left, right, start),
                consumer_process=signal.process_window(prom, 'consumer', [f'consumer-sts-{i}' for i in range(count)], left, right, start),
                producer_process=signal.process_window(prom, 'producer', manifest['producer_pods'], left, right, start))
        summary.update(backlog_condition=level, windows=windows, activation=activation,
            recovery_common_decision=recovery(lag['snapshots'], start+60, end, 100),
            decision_cohorts=json.loads((directory / 'decision-cohort-summary.json').read_text()),
            arrival_and_capacity={name:arrival_capacity(directory, manifest, lag, left, right, protocol['capacity_estimates'])
                                 for name, left, right in [('before', 30, 60), ('evaluation_end', 570, 600)]})
        rolling = [completion.window_metrics(messages, left, left+30, 1000) for left in (start+offset for offset in range(30, 571, 30))]
        curves['window_p99'] = [([(w['end_epoch']-start)/60 for w in rolling],
                                [w['completion_window_p99_seconds'] for w in rolling])]
        summaries.append(summary)
        plot_data.append((summary, curves, marks))
        del ack, done, messages
        print('[ANALYZED]', row['run_id'], flush=True)
    args.output.mkdir(parents=True, exist_ok=True)
    payload = dict(status='complete' if complete else 'provisional', completed_trials=len(summaries), planned_trials=len(expected),
        campaign_revision=state['code_commit'], analysis_revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        protocol=protocol, runs=summaries,
        definitions=dict(latency='Whole evaluation-production cohort, earliest completion by the drain cutoff; unfinished records are separate.',
            completion_windows='Before: seconds 30–60; after: verified activation to 600; final: 570–600. Throughput counts distinct completions in the interval; window latency uses evaluation messages completed there. All exclude drain.',
            signal_means='Equal-weight valid raw observations within each stated interval. Rolling signal plots retain their separately defined 15-observation and 15-interval windows.',
            recovery='Processing backlog at most 100 offsets for 30 continuous valid seconds during continuing input; elapsed time starts at the common scheduled decision.',
            repetitions='Individual results retained; no pooled p99 or average of rolling percentiles.'))
    (args.output / 'comparison.json').write_text(json.dumps(payload, indent=2, allow_nan=False)+'\n')
    plots(plot_data, args.output, complete)
    report(payload, args.output)


def plots(runs, output, complete):
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
    for key, title, ylabel in [
        ('lag', 'Runtime control signals: total lag', 'Offsets'),
        ('growth', 'Runtime control signals: processing-backlog growth', 'Offsets/s; rolling 30 s'),
        ('skew', 'Runtime control signals: mean lag skew', 'Maximum / mean partition lag'),
        ('throughput', 'Performance: distinct completion throughput', 'Messages/s; 10 s bins'),
        ('window_p99', 'Performance: completion-window p99', 'Seconds; 30 s completion intervals')]:
        fig, axes = plt.subplots(2, 2, figsize=(12, 7.6), sharex=True, sharey=True)
        handles = {}
        for s, data, _ in runs:
            ax = axes[0 if s['backlog_condition'] == 'smaller' else 1, s['run_number']-1]
            x, y = data[key][-1] if key in ('skew', 'throughput') else data[key][0]
            line, = ax.plot(x, y, label=LABELS[s['arm']], color=previous.COLORS[ARMS.index(s['arm'])], lw=1.2)
            handles[s['arm']] = line
        for i, row in enumerate(axes):
            for j, ax in enumerate(row):
                ax.axvline(1, color='black', ls='--', lw=.8)
                ax.set(title=f"{'Smaller' if i == 0 else 'Larger'} starting backlog — Run {j+1}",
                       xlabel='Minutes after evaluation starts', ylabel=ylabel, xlim=(0, 10))
                if key != 'growth':
                    ax.set_ylim(bottom=0)
                ax.grid(alpha=.2)
        fig.suptitle(title + ('' if complete else ' — provisional'))
        fig.legend([handles[a] for a in ARMS if a in handles], [LABELS[a] for a in ARMS if a in handles], loc='lower center', ncol=2)
        fig.tight_layout(rect=(0, .08, 1, .95))
        for extension in ('png', 'pdf'):
            fig.savefig(output / (key+'.'+extension), dpi=180)
        plt.close(fig)
    fig, axes = plt.subplots(2, 2, figsize=(12, 7.6), sharex=True, sharey=True)
    handles = {}
    for s, _, _ in runs:
        ax = axes[0 if s['backlog_condition'] == 'smaller' else 1, s['run_number']-1]
        values = s['decision_cohorts']['cohorts']['pending_at_decision']['completion_fraction_after_decision']
        line, = ax.step([v['seconds'] for v in values], [100*v['fraction'] for v in values], where='post',
                       label=LABELS[s['arm']], color=previous.COLORS[ARMS.index(s['arm'])])
        handles[s['arm']] = line
    for i, row in enumerate(axes):
        for j, ax in enumerate(row):
            ax.axvline(540, color='gray', ls='--', lw=.8)
            ax.set(title=f"{'Smaller' if i == 0 else 'Larger'} starting backlog — Run {j+1}",
                   xlabel='Seconds after scheduled decision', ylabel='Pending cohort completed (%)', ylim=(0, 102), xlim=(0, 660))
            ax.grid(alpha=.2)
    fig.suptitle('Performance: clearing work already pending at the decision' + ('' if complete else ' — provisional'))
    fig.legend([handles[a] for a in ARMS if a in handles], [LABELS[a] for a in ARMS if a in handles], loc='lower center', ncol=2)
    fig.tight_layout(rect=(0, .08, 1, .95))
    for extension in ('png', 'pdf'):
        fig.savefig(output / ('pending-clearance.'+extension), dpi=180)
    plt.close(fig)


def report(payload, output):
    lines = ['# Starting backlog and mitigation outcomes', '',
             f"Status: {payload['status']}. Completed trials: {payload['completed_trials']} of {payload['planned_trials']}.", '',
             'Every evaluation uses 600 total messages/s, with 80% sent to twelve of sixty partitions. Different warm-up rates establish two backlog conditions. These histories also change the age of queued records; they are not a pure manipulation of queue size.', '',
             '| Starting backlog | Response | Run | Completion p99 (s) | Unfinished (%) | Recovery confirmed (s) | Requested CPU (core-min) |',
             '| --- | --- | ---: | ---: | ---: | ---: | ---: |']
    for s in sorted(payload['runs'], key=lambda x:(x['backlog_condition'], ARMS.index(x['arm']), x['run_number'])):
        recovery_time = s['recovery_common_decision']['confirmation_after_anchor_seconds']
        recovery_text = f'{recovery_time:.1f}' if recovery_time is not None else 'Unconfirmed'
        cpu = s['requested_resources_evaluation_and_drain']['consumer_container_requested_cpu_seconds_observed']
        cpu_text = f'{cpu/60:.1f}' if cpu is not None else 'Unavailable'
        lines.append(f"| {s['backlog_condition'].capitalize()} | {LABELS[s['arm']]} | {s['run_number']} | {s['p99_seconds']:.3f} | {s['unfinished_percent']:.2f} | {recovery_text} | {cpu_text} |")
    lines += ['', *payload['definitions'].values(), '',
              'The pending-clearance curve uses all records acknowledged and still unfinished at the common decision time, including warm-up records. A plateau below 100% retains unfinished work in the denominator. The dashed line at 540 seconds marks production ending; the remaining 120 seconds are drain.', '',
              'Capacity estimates apply to the original three pods under the saved busy calibration. Added consumers remain uncalibrated. Two repetitions provide preliminary evidence, and shared-node variability remains a limitation.', '']
    (output / 'RESULTS.md').write_text('\n'.join(lines))


if __name__ == '__main__':
    main()
