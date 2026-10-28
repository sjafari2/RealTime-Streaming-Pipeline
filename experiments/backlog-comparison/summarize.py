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
INTERVALS = [('before', 'Before'), ('activation_to_end', 'Active–end'),
             ('evaluation_end', 'Eval. end')]


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
        completion_intervals = [completion.window_metrics(messages, left, left+30, 1000)
                                for left in (start+offset for offset in range(30, 571, 30))]
        curves['window_p99'] = [([(w['end_epoch']-start)/60 for w in completion_intervals],
                                [w['completion_window_p99_seconds'] for w in completion_intervals])]
        summaries.append(summary)
        plot_data.append((summary, curves, marks))
        del ack, done, messages
        print('[ANALYZED]', row['run_id'], flush=True)
    args.output.mkdir(parents=True, exist_ok=True)
    payload = dict(status='complete' if complete else 'provisional', completed_trials=len(summaries), planned_trials=len(expected),
        campaign_revision=state['code_commit'], analysis_revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
        protocol=protocol, runs=summaries,
        definitions=dict(latency='Whole evaluation-production cohort, earliest completion by the drain cutoff; unfinished records are separate.',
            completion_windows='Before: seconds 30–60; Active–end: verified activation to 600 (60–600 for Keep 3); Eval. end: seconds 570–600. Throughput counts distinct completions in the interval, including warm-up messages completed then. Interval p99 uses evaluation messages completed there, retaining their full creation-to-completion latency. All intervals exclude drain.',
            signal_means='Equal-weight valid raw observations within each stated interval. Rolling signal plots retain their separately defined 15-observation and 15-interval windows.',
            recovery='Processing backlog at most 100 offsets for 30 continuous valid seconds during continuing input; elapsed time starts at the common scheduled decision.',
            repetitions='Individual results retained; no pooled p99 or average of rolling percentiles.'))
    (args.output / 'comparison.json').write_text(json.dumps(payload, indent=2, allow_nan=False)+'\n')
    plots(plot_data, args.output, complete)
    interval_plots(summaries, args.output)
    report(payload, args.output)
    interval_report(payload, args.output)


def plots(runs, output, complete, metrics=None):
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MultipleLocator, StrMethodFormatter
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
    for key, title, ylabel in [
        ('lag', 'Performance Evaluation Metrics: total backlog', 'Offsets'),
        ('growth', 'Runtime control signals: processing-backlog growth', 'Offsets/s; rolling 30 s'),
        ('skew', 'Runtime control signals: mean lag skew', 'Maximum / mean partition lag'),
        ('throughput', 'Performance Evaluation Metrics: throughput', 'Messages/s; 10 s intervals'),
        ('window_p99', 'Performance Evaluation Metrics: p99 completion latency', 'Seconds; 30 s completion intervals')]:
        if metrics is not None and key not in metrics:
            continue
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
                if key == 'throughput':
                    ax.yaxis.set_major_locator(MultipleLocator(500))
                elif key == 'lag':
                    # Label the overview sparsely while retaining 2,500-offset ticks.
                    ax.yaxis.set_major_locator(MultipleLocator(25000))
                    ax.yaxis.set_minor_locator(MultipleLocator(2500))
                    ax.tick_params(axis='y', which='minor', length=3)
                ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}' if key in ('lag', 'throughput') else '{x:g}'))
                if key == 'window_p99':
                    for left, right, label in [(.5, 1, 'Before'), (9.5, 10, 'Eval. end')]:
                        ax.axvspan(left, right, color='#dddddd', alpha=.4, zorder=0)
                        ax.text((left+right)/2, .96, label, transform=ax.get_xaxis_transform(),
                                ha='center', va='top', fontsize=8)
                ax.grid(alpha=.2)
        fig.suptitle(title + ('' if complete else ' — provisional'))
        if key == 'lag':
            fig.text(.5, .925, 'Tick spacing: 2,500 offsets; labels every 25,000 offsets. Full data range shown.',
                     ha='center', fontsize=9)
        fig.legend([handles[a] for a in ARMS if a in handles], [LABELS[a] for a in ARMS if a in handles], loc='lower center', ncol=2)
        fig.tight_layout(rect=(0, .08, 1, .95))
        for extension in ('png', 'pdf'):
            fig.savefig(output / (key+'.'+extension), dpi=180)
        plt.close(fig)
    if metrics is not None:
        return
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
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), sharex=True, sharey=True)
    handles = {}
    for s, _, _ in runs:
        ax = axes[0 if s['backlog_condition'] == 'smaller' else 1]
        point = ax.scatter(s['p99_seconds'], s['unfinished_percent'], s=65,
                           marker='o' if s['run_number'] == 1 else 's',
                           color=previous.COLORS[ARMS.index(s['arm'])], label=LABELS[s['arm']])
        ax.annotate(str(s['run_number']), (s['p99_seconds'], s['unfinished_percent']),
                    xytext=(4, 5), textcoords='offset points', fontsize=9)
        handles[s['arm']] = point
    for ax, level in zip(axes, ('Smaller', 'Larger')):
        ax.set(title=level+' starting backlog', xlabel='Completed-message p99 (s)',
               ylabel='Unfinished evaluation messages (%)')
        ax.grid(alpha=.2)
    fig.suptitle('Performance: completion latency alongside unfinished work' + ('' if complete else ' — provisional'))
    fig.legend([handles[a] for a in ARMS if a in handles], [LABELS[a] for a in ARMS if a in handles], loc='lower center', ncol=2)
    fig.tight_layout(rect=(0, .13, 1, .93))
    for extension in ('png', 'pdf'):
        fig.savefig(output / ('latency-and-unfinished.'+extension), dpi=180)
    plt.close(fig)


def interval_plots(summaries, output):
    """Use the proposal's named intervals without relabelling an entire time series."""
    import matplotlib.pyplot as plt
    for metric, title, unit, filename in [
        ('completion_window_p99_seconds', 'p99 completion latency', 'Seconds', 'p99-reporting-intervals'),
        ('completion_throughput_per_second', 'throughput', 'Messages/s', 'throughput-reporting-intervals')]:
        fig, axes = plt.subplots(2, 3, figsize=(13, 8.3))
        for row, level in enumerate(('smaller', 'larger')):
            for column, (key, label) in enumerate(INTERVALS):
                ax = axes[row, column]
                observed = []
                for s in summaries:
                    if s['backlog_condition'] != level:
                        continue
                    index = ARMS.index(s['arm'])
                    value = s['windows'][key]['completion'][metric]
                    if value is None:
                        continue
                    observed.append(value)
                    shift = -.11 if s['run_number'] == 1 else .11
                    ax.scatter(index+shift, value, marker='o' if s['run_number'] == 1 else 's',
                               color=previous.COLORS[index], s=35, zorder=3, clip_on=False)
                    if metric == 'completion_window_p99_seconds':
                        ax.annotate(f'{value:.3f}' if value < 1 else f'{value:.1f}',
                                    (index+shift, value), xytext=(-3 if shift < 0 else 3, 7),
                                    textcoords='offset points', ha='right' if shift < 0 else 'left',
                                    fontsize=7, color=previous.COLORS[index])
                ax.set(title=f'{level.capitalize()} backlog — {label}', ylabel=unit,
                       xticks=range(4), xticklabels=['Keep 3', 'Redistribute\n3', 'Targeted\nscale 6', 'Kafka\nscale 6'])
                ax.set_xlim(-.5, 3.5)
                ax.set_ylim(0, max(observed)*1.15 if observed and max(observed) > 0 else 1)
                ax.grid(axis='y', alpha=.2)
        from matplotlib.lines import Line2D
        fig.legend([Line2D([], [], marker=m, color='#444444', linestyle='None') for m in ('o', 's')],
                   ['Run 1', 'Run 2'], loc='lower center', ncol=2)
        fig.suptitle('Performance Evaluation Metrics: '+title)
        fig.text(.5, .938, 'Before: 30–60 s   |   Active–end: confirmed activation–600 s   |   Eval. end: 570–600 s',
                 ha='center', fontsize=10)
        fig.text(.5, .903, 'Times are relative to evaluation start; drain excluded. Keep 3 uses 60–600 s for Active–end.',
                 ha='center', fontsize=9)
        note = ('P99 includes completed evaluation messages only; pending work is reported separately. Panel scales vary.'
                if metric == 'completion_window_p99_seconds' else
                'Throughput includes warm-up messages completed within each interval. Panel scales vary.')
        fig.text(.5, .057, note, ha='center', fontsize=9)
        fig.tight_layout(rect=(0, .085, 1, .89))
        for extension in ('png', 'pdf'):
            fig.savefig(output / (filename+'.'+extension), dpi=180)
        plt.close(fig)


def interval_report(payload, output):
    """Export unrounded interval values, and the ownership evidence behind recovery."""
    lines = ['# Performance and signals by reporting interval', '', payload['definitions']['completion_windows'], '',
             'Mean backlog and mean skew use equally weighted valid original observations. Pending counts refer to evaluation messages at each interval endpoint, not unfinished messages after drain.', '']
    records = []
    for level in ('smaller', 'larger'):
        for number in (1, 2):
            lines += [f'## {level.capitalize()} starting backlog, Run {number}', '',
                '| Response | Interval | Throughput (msg/s) | p99 (s) | Mean backlog (offsets) | Mean skew | Pending at endpoint | Valid/expected signal samples |',
                '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |']
            for s in sorted(payload['runs'], key=lambda x: ARMS.index(x['arm'])):
                if s['backlog_condition'] != level or s['run_number'] != number:
                    continue
                for key, label in INTERVALS:
                    window = s['windows'][key]
                    c, signals = window['completion'], window['signals']
                    fields = signals['fields']
                    record = dict(backlog_condition=level, run_number=number, run_id=s['run_id'], arm=s['arm'],
                        interval=label, start_epoch=c['start_epoch'], end_epoch=c['end_epoch'],
                        throughput=c['completion_throughput_per_second'], p99_seconds=c['completion_window_p99_seconds'],
                        mean_backlog=fields.get('total_lag', {}).get('mean'),
                        mean_skew=fields.get('skew', {}).get('mean'),
                        pending_at_endpoint=c['acknowledged_evaluation_messages_pending_at_end'],
                        valid_signal_samples=signals['valid_observations'], expected_signal_samples=signals['expected_observations'])
                    records.append(record)
                    def fmt(value, digits=2):
                        return f'{value:,.{digits}f}' if value is not None else 'Unavailable'
                    lines.append(f"| {LABELS[s['arm']]} | {label} | {fmt(record['throughput'])} | {fmt(record['p99_seconds'],3)} | {fmt(record['mean_backlog'],1)} | {fmt(record['mean_skew'])} | {record['pending_at_endpoint']:,} | {record['valid_signal_samples']}/{record['expected_signal_samples']} |")
            lines.append('')
    lines += ['## Assignment after scaling', '',
              'These counts describe high-traffic partitions specified by the workload, not partitions classified as hot from measured lag. Each native Kafka scaling run ended with ten total partitions per consumer.', '',
              '| Starting backlog | Run | C0 | C1 | C2 | C3 | C4 | C5 |',
              '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for s in payload['runs']:
        if s['arm'] == 'kafka_scale6':
            counts = s['final_hot_partitions_per_owner']
            lines.append(f"| {s['backlog_condition'].capitalize()} | {s['run_number']} | " +
                         ' | '.join(str(counts.get(f'consumer-sts-{i}', 0)) for i in range(6)) + ' |')
    lines += ['', 'Starting ownership and the original consumers were matched. Native scaling produced different final assignments, and added-consumer nodes could differ. These differences prevent attributing recovery differences solely to starting backlog.', '',
              'Within each backlog condition, all four responses use the same input and three-consumer assignment before the action at 60 seconds. Similar initial trajectories are therefore expected. Smaller and larger preparations differ in warm-up input and retained backlog; evaluation input is 600 messages/s for both.', '']
    (output / 'reporting-intervals.json').write_text(json.dumps(records, indent=2, allow_nan=False)+'\n')
    (output / 'REPORTING_INTERVALS.md').write_text('\n'.join(lines))


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
              'Capacity estimates apply to the original three pods under the saved busy calibration. Added consumers remain uncalibrated. Individual repetitions are retained; limited repetitions and shared-node variability constrain interpretation.', '',
              'See [reporting intervals](REPORTING_INTERVALS.md) for Before, Active–end and Eval. end throughput, p99, mean backlog, mean skew and pending messages, together with native scaling assignments.', '']
    (output / 'RESULTS.md').write_text('\n'.join(lines))


if __name__ == '__main__':
    main()
