"""Run the twelve-partition comparison with complete timing evidence at 600 msg/s."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def preserve(directory, backup_root):
    """Retain a verified second local copy without replacing earlier evidence."""
    destination = backup_root / directory.name
    hashes = {}
    for source in sorted(directory.rglob('*')):
        if not source.is_file():
            continue
        relative = source.relative_to(directory)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        expected = digest(source)
        if target.exists() and digest(target) != expected:
            raise RuntimeError('Backup conflict: ' + str(target))
        if not target.exists():
            shutil.copy2(source, target)
        if digest(target) != expected:
            raise RuntimeError('Backup verification failed: ' + str(target))
        hashes[str(relative)] = expected
    (destination / 'backup-sha256.json').write_text(json.dumps(hashes, indent=2) + '\n')
    return destination


def load_base():
    spec = importlib.util.spec_from_file_location('twelve_hot_base', ROOT / 'experiments/four-condition-20260929/run.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def use_local_evidence(base, evidence_root):
    """Keep live files on local storage while retaining the existing result paths."""
    evidence_root = evidence_root.resolve()
    evidence_root.mkdir(parents=True, exist_ok=True)
    original_journal = base.r.journal

    def journal(control, filename, row):
        run_id = control['run_id']
        if not re.fullmatch(r'run-\d{8}-\d{6}', run_id):
            raise ValueError('Unexpected run identifier')
        target = evidence_root / run_id
        link = ROOT / 'results' / run_id
        if link.is_symlink():
            if link.resolve() != target:
                raise RuntimeError('Result path points to different storage: ' + str(link))
        elif link.exists():
            raise RuntimeError('Refusing to replace existing run evidence: ' + str(link))
        else:
            if target.exists():
                raise RuntimeError('Local evidence already exists: ' + str(target))
            target.mkdir()
            link.symlink_to(target, target_is_directory=True)
        original_journal(control, filename, row)

    base.r.journal = journal


def install(base, calibration_reference):
    original_protocol = base.campaign_protocol
    original_prepared = base.prepared

    def protocol(aggregate_rate=600, targeted_only=False, hot_count=12, deadline_ms=1000):
        if (aggregate_rate, targeted_only, hot_count, deadline_ms) != (600, False, 12, 1000):
            raise ValueError('This comparison is fixed at 600 messages/s, twelve high-input partitions and four responses')
        result = original_protocol(600, False, 12, 1000)
        result.update(calibration_reference=calibration_reference,
            study_purpose='Compare four scheduled responses with twelve high-input partitions at a common evaluation input of 600 messages/s. Retain earlier 600 campaigns as separate deployment blocks.',
            slo_rationale='One second is the experimental deadline, with 0.5 seconds retained as a sensitivity threshold. Balanced backlog stability does not establish compliance with this deadline.',
            lag_analysis_parameters=dict(window_samples=15, growth_window_samples=15,
                export_step_seconds=2, hot_k=1., minimum_lag=10., persistence=.8, max_gap_seconds=3.))
        result['outcomes'] = [item.replace('10-second growth', '30-second growth') for item in result['outcomes']]
        result['performance_windows'].update(
            rolling='Backlog and skew use 15 valid observations at two-second steps. Growth uses 15 differences. Completion plots use consecutive nonoverlapping 30-second intervals.',
            after='Confirmed activation through evaluation second 600. Keep 3 uses seconds 60 through 600. Drain ends at evaluation second 720 and is a separate reporting boundary.')
        result['timing_evidence'] = {
            'preserve': ['message creation and acknowledgment', 'processing start and completion',
                'partition and offset', 'consumer process identity', 'ownership callbacks',
                'scheduled and actual action request', 'replica readiness', 'release, acquire and resume',
                'verified activation', 'evaluation start, production stop and drain cutoff',
                'raw monitoring samples', 'clock-probe uncertainty bounds'],
            'interruption': 'Keep global no-processing intervals and per-partition handover intervals separate from request-to-activation delay. Preparation can overlap continued processing.',
            'later_analysis': 'Raw evidence supports consistently defined additional intervals. Preserve the original cutoffs and disclose changed reporting definitions. Do not choose intervals independently for favorable outcomes.'}
        return result

    def prepared(*args, **kwargs):
        config, plan = original_prepared(*args, **kwargs)
        config['data']['SEPARATE_BROKER_NODES'] = 'false'
        return config, plan

    base.campaign_protocol = protocol
    base.prepared = prepared
    return protocol()


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--calibration-reference', required=True)
    parser.add_argument('--backup-root', type=Path, required=True)
    parser.add_argument('--analysis-output', type=Path, required=True)
    parser.add_argument('--evidence-root', type=Path,
                        help='Local directory outside cloud storage for live run evidence')
    args, remaining = parser.parse_known_args()
    base = load_base()
    protocol = install(base, args.calibration_reference)
    if '--execute' not in remaining:
        print(json.dumps(protocol, indent=2))
        return
    if args.evidence_root:
        use_local_evidence(base, args.evidence_root)
    from compress_evidence import compress
    from compress_monitoring_csv import compress_csv
    original_complete = base.r.complete_run

    def complete(plan=None):
        directory = original_complete(plan)
        if not (plan or {}).get('prepare_only'):
            compress(directory)
            compress_csv(directory)
        preserve(directory, args.backup_root)
        return directory

    base.r.complete_run = complete
    sys.argv = [sys.argv[0], '--aggregate-rate', '600', '--hot-partitions', '12', '--slo-ms', '1000'] + remaining
    audit = base.main()
    preserve(audit, args.backup_root)
    subprocess.run([sys.executable, str(ROOT / 'experiments/four-condition-20260929/summarize.py'),
                    str(audit), str(args.analysis_output), '--growth-window-samples', '15'], check=True)


if __name__ == '__main__':
    main()
