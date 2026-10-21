"""Compare observed completions before an action and at evaluation end.

These retrospective window statistics supplement the production-cohort results.
They never replace whole-run p99 or the unfinished count at drain end.
"""
import argparse
import bisect
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from evidence_io import event_paths, open_events

ARMS = ('keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6')
LABELS = dict(zip(ARMS, ('Keep 3', 'Redistribute 3', 'Targeted scale 6', 'Kafka scale 6')))
COLORS = dict(zip(ARMS, ('#2765a6', '#d97b1d', '#168361', '#9653a6')))


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def p99(values):
    """Nearest-rank percentile of individual latencies, not of percentiles."""
    ordered = sorted(values)
    return ordered[math.ceil(.99 * len(ordered)) - 1] if ordered else None


def load_messages(directory, manifest, expected):
    acknowledged, completed, hashes = {}, {}, {}
    seen_pods = {'producer': set(), 'consumer': set()}
    for path in event_paths(directory):
        relative = str(path.relative_to(directory))
        hashes[relative] = sha(path)
        if hashes[relative] != expected.get(relative):
            raise ValueError('Message evidence hash mismatch: ' + relative)
        final = json.loads(path.with_name('final.json').read_text())
        if final['run_id'] != manifest['run_id'] or any(final.get(k) for k in
                ('failure', 'evidence_dropped', 'evidence_error')):
            raise ValueError('Incomplete or inconsistent final status: ' + relative)
        seen_pods[final['role']].add(final['pod'])
        with open_events(path) as stream:
            for line in stream:
                event = json.loads(line)
                kind = event['event']
                if kind == 'started' and not event.get('outcomes_enabled', False):
                    raise ValueError('Outcome logging was disabled')
                if kind == 'acknowledged':
                    identity = event['message_id']
                    value = (event['producer_timestamp'], event['timestamp'])
                    if identity in acknowledged and acknowledged[identity][0] != value[0]:
                        raise ValueError('Conflicting producer timestamps')
                    old = acknowledged.get(identity, value)
                    acknowledged[identity] = (value[0], min(old[1], value[1]))
                elif kind == 'completed':
                    identity = event['message_id']
                    value = (event['completion_timestamp'], event['producer_timestamp'], event['output_sha256'])
                    old = completed.get(identity)
                    if old and old[1:] != value[1:]:
                        raise ValueError('Conflicting completion identity or output')
                    if old is None or value[0] < old[0]:
                        completed[identity] = value
    if hashes != expected:
        raise ValueError('Missing or unexpected message evidence files')
    for role, pods in seen_pods.items():
        if not set(manifest[role + '_pods']).issubset(pods):
            raise ValueError('Missing evidence for initial ' + role + ' pods')
    if not set(completed).issubset(acknowledged):
        raise ValueError('Completion without a producer acknowledgment')
    for identity, (finished, produced, _) in completed.items():
        if produced != acknowledged[identity][0] or not math.isfinite(finished-produced) or finished < produced:
            raise ValueError('Invalid completion timestamp')
    return acknowledged, completed, hashes


def build_series(acknowledged, completed, start, end):
    """Sort once so overlapping windows select original message observations."""
    all_completions = sorted(value[0] for value in completed.values())
    evaluation_completions = sorted((value[0], value[0]-acknowledged[key][0])
        for key, value in completed.items() if start <= acknowledged[key][0] < end)
    return dict(all_completions=all_completions,
        evaluation_completions=evaluation_completions,
        evaluation_completion_times=[v[0] for v in evaluation_completions],
        input_times=sorted(v[0] for v in acknowledged.values()),
        evaluation_ack_times=sorted(v[1] for v in acknowledged.values() if start <= v[0] < end),
        evaluation_settled_times=sorted(max(acknowledged[k][1], v[0]) for k, v in completed.items()
            if start <= acknowledged[k][0] < end))


def window_metrics(series, left, right):
    if not math.isfinite(left) or not math.isfinite(right) or right <= left:
        raise ValueError('Window must have a positive finite duration')
    def count(values):
        return bisect.bisect_left(values, right)-bisect.bisect_left(values, left)
    times = series['evaluation_completion_times']
    lo, hi = bisect.bisect_left(times, left), bisect.bisect_left(times, right)
    latencies = [v[1] for v in series['evaluation_completions'][lo:hi]]
    completions = count(series['all_completions'])
    return dict(start_epoch=left, end_epoch=right, duration_seconds=right-left,
        distinct_completions=completions, completion_throughput_per_second=completions/(right-left),
        acknowledged_input_per_second=count(series['input_times'])/(right-left),
        evaluation_completions_for_latency=len(latencies),
        completion_window_p99_seconds=p99(latencies),
        completion_window_mean_seconds=sum(latencies)/len(latencies) if latencies else None,
        acknowledged_evaluation_messages_pending_at_end=
            bisect.bisect_left(series['evaluation_ack_times'], right)
            -bisect.bisect_left(series['evaluation_settled_times'], right))


def analyze(comparison_path, repository):
    source = json.loads(comparison_path.read_text())
    if sorted((r['arm'], r['run_number']) for r in source['runs']) != sorted((a, n) for a in ARMS for n in (1, 2)):
        raise ValueError('Expected the eight twelve-partition trials')
    results = []
    for row in source['runs']:
        print('Analyzing', row['run_id'], flush=True)
        directory = repository / row['raw_evidence']
        for filename, expected in row['evidence_hashes'].items():
            if sha(directory / filename) != expected:
                raise ValueError('Evidence hash changed: ' + filename)
        manifest = json.loads((directory/'manifest.json').read_text())
        start, end, cutoff = (manifest[k] for k in ('evaluation_start_epoch', 'producer_end_epoch', 'drain_end_epoch'))
        if not math.isclose(end-start, 600):
            raise ValueError('This comparison requires ten-minute evaluation')
        ack, done, hashes = load_messages(directory, manifest, row['intervention_cost']['event_file_sha256'])
        admitted = {key: value for key, value in ack.items() if start <= value[0] < end}
        full_latencies = [done[k][0]-v[0] for k, v in admitted.items() if k in done and done[k][0] <= cutoff]
        series = build_series(ack, done, start, end)
        whole = window_metrics(series, start, end)
        if (len(admitted) != row['admitted'] or len(full_latencies) != row['completed']
                or len(admitted)-len(full_latencies) != row['unfinished']
                or not math.isclose(p99(full_latencies), row['p99_seconds'], abs_tol=1e-12)
                or not math.isclose(whole['completion_throughput_per_second'], row['useful_throughput'], abs_tol=1e-12)):
            raise ValueError('Replayed outcomes differ from the original comparison')
        windows = {}
        for label, left, right in [('before', 30, 60), ('evaluation_end', 570, 600)]:
            windows[label] = dict(window_metrics(series, start+left, start+right),
                start_after_evaluation_seconds=left, end_after_evaluation_seconds=right)
        rolling = [dict(window_metrics(series, start+t-30, start+t),
            end_after_evaluation_seconds=t) for t in range(30, 601, 2)]
        results.append(dict(arm=row['arm'], run_number=row['run_number'], run_id=row['run_id'],
            windows=windows, rolling=rolling, evidence_sha256=row['evidence_hashes'],
            event_file_sha256=hashes, whole_run_replay_matches=True,
            whole_run_unfinished=row['unfinished'], whole_run_unfinished_percent=row['unfinished_percent']))
        print('  before:', windows['before']['completion_throughput_per_second'], windows['before']['completion_window_p99_seconds'],
              'end:', windows['evaluation_end']['completion_throughput_per_second'], windows['evaluation_end']['completion_window_p99_seconds'], flush=True)
    return dict(schema_version=1, comparison_sha256=sha(comparison_path), analysis_script_sha256=sha(__file__),
        analysis_source_revision=subprocess.check_output(['git','-C',str(repository),'rev-parse','HEAD'],text=True).strip(),
        scope='Retrospective completion-window diagnostic for the eight twelve-high-input-partition trials; not a replacement for whole-evaluation production-cohort outcomes.',
        definition='Throughput counts earliest distinct completions in [left,right), including warm-up records; p99 uses individual latencies of acknowledged evaluation-born messages whose earliest completion falls in that interval. Nearest rank; no averaging of percentiles. Completion precedes commit acknowledgment.',
        windows='Before: evaluation seconds [30,60). Evaluation end: [570,600), before drain. The same windows apply to Keep 3. Rolling traces use trailing 30-second windows at 2-second steps, plotted at the window end.',
        limitations='Only completed evaluation records contribute to window p99. Pending records are reported alongside it. Latency uses original unadjusted producer and consumer clocks, so small differences require caution. Later recovery must not be inferred from p99 alone.',
        runs=results)


def write_outputs(data, output):
    output.mkdir(parents=True, exist_ok=True)
    (output/'action-window-performance.json').write_text(json.dumps(data, indent=2, allow_nan=False)+'\n')
    lines=['# Performance before intervention and at evaluation end', '', data['scope'], '', data['definition'], '', data['windows'], '',
        '| Response | Run | Before throughput (msg/s) | End throughput (msg/s) | Before p99 (s) | End p99 (s) | Pending evaluation messages, before / end |',
        '|---|---:|---:|---:|---:|---:|---:|']
    latex=[]
    for r in data['runs']:
        b,e=r['windows']['before'],r['windows']['evaluation_end']
        values=[b['completion_throughput_per_second'],e['completion_throughput_per_second'],b['completion_window_p99_seconds'],e['completion_window_p99_seconds']]
        text=['---' if v is None else f'{v:.3f}' for v in values]
        lines.append('| '+LABELS[r['arm']]+' | '+str(r['run_number'])+' | '+' | '.join(text)+f" | {b['acknowledged_evaluation_messages_pending_at_end']:,} / {e['acknowledged_evaluation_messages_pending_at_end']:,} |")
        latex.append(' & '.join([LABELS[r['arm']],str(r['run_number']),*text])+r' \\')
    lines += ['', data['limitations'], '', 'All event and summary hashes were checked against the published comparison. Admission, completion, unfinished, whole-run p99 and throughput were replayed and matched before calculating windows.', '',
        '![Completion throughput, p99 and pending work](figures/twelve-partition-action-windows.png)', '',
        'Reproduce from the retained raw evidence at the repository root:', '', '```bash',
        'python3 experiments/four-condition-20260929/analyze_action_windows.py \\',
        '  experiment-records/four-condition-20260929/comparison.json \\',
        '  --output /path/to/analysis-output', '```', '',
        'The output directory contains the numerical record, table rows and PNG/PDF plots. No cluster access or new performance trial is required.', '']
    (output/'ACTION_WINDOWS.md').write_text('\n'.join(lines))
    (output/'action-window-table-rows.tex').write_text('\n'.join(latex)+'\n')


def plot(data, output):
    output = output/'figures'
    output.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault('MPLCONFIGDIR','/private/tmp/action-window-mpl')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    fig,axes=plt.subplots(3,2,figsize=(10,9),sharex=True,sharey='row')
    for r in data['runs']:
        col=r['run_number']-1; points=r['rolling']; x=[p['end_after_evaluation_seconds']/60 for p in points]
        for i,key in enumerate(('completion_throughput_per_second','completion_window_p99_seconds','acknowledged_evaluation_messages_pending_at_end')):
            axes[i,col].plot(x,[p[key] for p in points],color=COLORS[r['arm']],lw=1.5)
    for col in range(2):
        axes[0,col].set_title(f'Run {col+1}',fontsize=12)
        axes[0,col].axhline(700,color='#777777',ls='--',lw=.8)
        axes[1,col].set_yscale('log')
        axes[2,col].set_yscale('symlog',linthresh=10)
        axes[2,col].set_xlabel('Minutes since evaluation began')
        for ax in axes[:,col]:
            ax.axvspan(.5,1,color='#a1a1a1',alpha=.14)
            ax.axvspan(9.5,10,color='#a1a1a1',alpha=.14)
            ax.axvline(1,color='#333333',ls=':',lw=1)
            ax.grid(alpha=.2);ax.set_xlim(0,10)
        axes[0,col].set_ylim(bottom=0)
        axes[2,col].set_ylim(bottom=0)
    axes[0,0].set_ylabel('Completion throughput\n(messages/s)')
    axes[1,0].set_ylabel('Completed-message p99\n(seconds; logarithmic)')
    axes[2,0].set_ylabel('Pending evaluation messages\n(linear below 10; log above)')
    fig.suptitle('Twelve high-input partitions: performance around intervention',fontsize=14,y=.985)
    handles=[Line2D([0],[0],color=COLORS[a],lw=2,label=LABELS[a]) for a in ARMS]
    fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.52,.952),ncol=4,frameon=False,fontsize=10)
    fig.subplots_adjust(left=.12,right=.98,bottom=.17,top=.885,hspace=.22,wspace=.1)
    fig.text(.52,.035,'Top two rows: trailing 30-second windows, plotted every 2 seconds at window end.\n'
        'P99 excludes warm-up records; throughput includes their completions. Pending count is instantaneous.\n'
        'Dotted: scheduled action at minute 1. Shaded: table windows. Dashed: 700 messages/s target input.',ha='center',fontsize=9)
    for suffix in ('png','pdf'):
        fig.savefig(output/('twelve-partition-action-windows.'+suffix),dpi=180)
    plt.close(fig)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('comparison',type=Path);p.add_argument('--repository',type=Path,default=ROOT)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--reuse',action='store_true')
    args=p.parse_args()
    if args.reuse:
        data=json.loads((args.output/'action-window-performance.json').read_text())
    else:
        data=analyze(args.comparison,args.repository)
        write_outputs(data,args.output)
    plot(data,args.output)


if __name__=='__main__': main()
