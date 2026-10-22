"""Compare sampled backlog and skew before action, after activation and at evaluation end."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    'signal_windows', ROOT/'experiments/four-hot600-20261021/analyze_signal_windows.py')
signal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal)

GROUPS = (
    ('12hot-700', 'experiment-records/four-condition-20260929/comparison.json',
     'experiment-records/four-condition-20260929/action-window-performance.json', 8),
    ('4hot-700', 'experiment-records/four-hot-partitions-20260930/comparison.json',
     'experiment-records/four-hot-rate-comparison-20261021/historical-completion-windows.json', 4),
    ('4hot-600', 'results/four-hot600-20261021-analysis/comparison.json',
     'experiment-records/four-hot600-20261021/completion-windows.json', 8),
)
WINDOWS = ('before', 'post_action_to_evaluation_end', 'evaluation_end')
NAMES = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute 3',
         'scale_redistribute6': 'Targeted scale 6', 'kafka_scale6': 'Kafka scale 6'}


def build(roots):
    hashes = {}

    def read(relative, expected=None):
        path = next((root/relative for root in roots if (root/relative).is_file()), None)
        if path is None:
            raise FileNotFoundError(relative)
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if expected and digest != expected:
            raise ValueError('Evidence changed: ' + relative)
        hashes[relative] = digest
        return json.loads(raw)

    runs = []
    for group, comparison_path, windows_path, count in GROUPS:
        comparison = read(comparison_path)
        completion = read(windows_path)
        if completion['comparison_sha256'] != hashes[comparison_path]:
            raise ValueError('Completion windows refer to a different comparison')
        timing = {row['run_id']: row for row in completion['runs']}
        if len(comparison['runs']) != count or len(timing) != count:
            raise ValueError('Unexpected trial count for ' + group)
        for run in comparison['runs']:
            prefix = run['raw_evidence'] + '/'
            manifest = read(prefix+'manifest.json', run['evidence_hashes']['manifest.json'])
            lag = read(prefix+'lag-summary.json', run['evidence_hashes']['lag-summary.json'])
            origin = manifest['evaluation_start_epoch']
            if manifest['producer_end_epoch'] - origin != 600:
                raise ValueError('Expected a 600-second evaluation')
            windows = {}
            for key in WINDOWS:
                bounds = timing[run['run_id']]['windows'][key]
                left, right = bounds['start_epoch'], bounds['end_epoch']
                summary = signal.signal_window(lag['snapshots'], left, right, origin)
                for field in ('total_lag', 'skew'):
                    if summary['fields'][field]['samples'] != summary['valid_observations']:
                        raise ValueError('Valid snapshot lacks finite ' + field)
                windows[key] = dict(
                    start_seconds=left-origin, end_seconds=right-origin,
                    mean_backlog_offsets=summary['fields']['total_lag']['mean'],
                    mean_skew=summary['fields']['skew']['mean'],
                    valid_samples=summary['valid_observations'],
                    expected_samples=summary['expected_observations'],
                    sample_coverage_fraction=summary['valid_grid_fraction'])
            runs.append(dict(group=group, arm=run['arm'], run_number=run['run_number'],
                             run_id=run['run_id'], windows=windows))
    return dict(schema_version=1,
        definition='Arithmetic means of valid raw total lag B=sum(partition lag) and skew=max(partition lag)/mean(partition lag), not time-weighted means or means of rolling means.',
        intervals='Before uses nominal seconds 30,32,...,58; activation-to-end uses query samples at or after the unrounded verified activation and before second 600 (Keep 3 starts at 60); final uses 570,572,...,598. All exclude drain.',
        coverage='Valid samples / expected two-second query samples, including completely absent grid points in the denominator. No zero fill, interpolation or requirement for a new rolling window after a gap.',
        correction='Exported timestamps have millisecond precision. Samples within 1 ms of the nominal two-second query grid are assigned to that grid before interval selection. This prevents an unintended one-sample shift in windows when export rounding precedes a boundary; completion-event intervals and original outcomes are unchanged.',
        limitations='Skew is a relative concentration measure: a large ratio can coexist with a very small total backlog. Read backlog and skew together. Post-activation means include recovery and may exceed final-window means. Rates and deployments are separate comparison blocks.',
        source_sha256=hashes, runs=runs)


def write_report(data, path):
    lines = ['# Backlog and skew in the performance intervals', '']
    for key in ('definition', 'intervals', 'coverage', 'correction', 'limitations'):
        lines += [data[key], '']
    for group, *_ in GROUPS:
        lines += ['## ' + group, '', '| Response | Run | Interval | Mean backlog (offsets) | Mean skew | Valid / expected samples |',
                  '| --- | --- | --- | ---: | ---: | ---: |']
        for run in data['runs']:
            if run['group'] != group:
                continue
            for key in WINDOWS:
                w = run['windows'][key]
                label = dict(before='Before', post_action_to_evaluation_end='Activation to end', evaluation_end='Final 30 s')[key]
                lines += [f"| {NAMES[run['arm']]} | {run['run_number']} | {label} | {w['mean_backlog_offsets']:,.1f} | {w['mean_skew']:.2f} | {w['valid_samples']}/{w['expected_samples']} |"]
        lines += ['']
    path.write_text('\n'.join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=ROOT)
    parser.add_argument('--evidence-root', type=Path, action='append', default=[])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    data = build(args.evidence_root + [args.repository])
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output/'window-signals.json').write_text(json.dumps(data, indent=2, allow_nan=False)+'\n')
    write_report(data, args.output/'WINDOW_SIGNALS.md')


if __name__ == '__main__':
    main()
