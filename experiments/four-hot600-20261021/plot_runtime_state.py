"""Plot the proposal's windowed lag state from verified campaign observations."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison', type=Path)
    parser.add_argument('--repository', type=Path, default=ROOT)
    parser.add_argument('--raw-root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raw = args.raw_root or args.repository
    sys.path.insert(0, str(args.repository/'experiments/result-metric-audit-20260930'))
    from build_compact_figures import runtime_states, runtime_figure
    data = json.loads(args.comparison.read_text())
    rows, states, hashes = [], {}, {}
    rate = data['cost_protocol']['aggregate_input_rate']
    hot_count = len(data['hot_partitions'])
    for trial in data['runs']:
        directory = raw/trial['raw_evidence']
        for name in ('manifest.json', 'lag-summary.json'):
            actual = sha(directory/name)
            if actual != trial['evidence_hashes'][name]:
                raise ValueError('Evidence changed: '+str(directory/name))
            hashes[trial['run_id']+'/'+name] = actual
        manifest = json.loads((directory/'manifest.json').read_text())
        lag = json.loads((directory/'lag-summary.json').read_text())
        parameters = lag['parameters']
        if parameters['window_samples'] != 15 or parameters.get('growth_window_samples', 15) != 15:
            raise ValueError('The runtime-state figure requires the original 15-sample/interval windows')
        start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
        rows.append(dict(run_id=trial['run_id'], arm=trial['arm'],
                         run_number=trial['run_number'], evaluation_seconds=end-start))
        states[trial['run_id']] = dict(arm=trial['arm'], run_number=trial['run_number'],
            parameters=parameters, states=runtime_states(lag['snapshots'], start, end))
    if len(rows) != 8 or {(r['arm'], r['run_number']) for r in rows} != {
            (arm, run) for arm in ('keep3','redistribute3','scale_redistribute6','kafka_scale6')
            for run in (1,2)}:
        raise ValueError('Expected four responses with two distinct trials each')
    if len(states) != 8:
        raise ValueError('Duplicate trial identity')
    args.output.mkdir(parents=True, exist_ok=True)
    title = f'{hot_count} high-input partitions at {rate} messages/s — runtime state'
    runtime_figure(rows, states, title, args.output/'runtime-state', expanded=True)
    record = dict(comparison_sha256=sha(args.comparison), evidence_sha256=hashes,
        plotting_source_sha256={name:sha(args.repository/'experiments/result-metric-audit-20260930'/name)
                               for name in ('build_compact_figures.py','build_report.py')},
        definition='Same reconstruction as the historical runtime-state figure: backlog, skew and persistence use 15 observations; growth spans 15 two-second intervals. Missing observations and ownership transitions break curves. This is a retrospective signal analysis, not evidence of an adaptive controller.',
        aggregate_input_rate=rate, hot_partition_count=hot_count, runs=states)
    (args.output/'runtime-states.json').write_text(json.dumps(record, indent=2, allow_nan=False)+'\n')
    print('Saved runtime-state figure for eight verified trials.')


if __name__ == '__main__':
    main()
