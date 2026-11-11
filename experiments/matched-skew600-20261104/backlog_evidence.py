"""Check logged input traces before starting the backlog mitigation comparisons."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path

from evidence_io import event_paths


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def check_preparation(controls):
    selected = {c['condition']: c for c in controls
                if c.get('condition') in ('smaller', 'larger') and c['report']['status'] == 'passed'}
    if set(selected) != {'smaller', 'larger'}:
        raise ValueError('Both validated preparation controls are required')
    sources = {}
    for condition, control in selected.items():
        directory = Path(control['directory'])
        manifest = json.loads((directory / 'manifest.json').read_text())
        outcome = json.loads((directory / 'outcome-summary.json').read_text())
        if outcome.get('validity_failures') or abs(outcome['admitted_messages_per_second'] / 600 - 1) > .01:
            raise ValueError('Continuing input is invalid or not within 1 percent of 600 msg/s')
        if set(manifest['producer_pods']) != {'producer-workers-' + str(i) for i in range(3)}:
            raise ValueError('Three original producer traces are required')
        hashes = {str(path.relative_to(directory)): digest(path)
                  for path in event_paths(directory / 'producer')}
        sources[condition] = dict(directory=str(directory), run_id=manifest['run_id'],
            event_sha256=hashes, manifest_sha256=digest(directory / 'manifest.json'),
            outcome_sha256=digest(directory / 'outcome-summary.json'))
    root = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
    spec = importlib.util.spec_from_file_location('backlog_trace_check', root / 'experiments/backlog-comparison/verify_workload.py')
    checks = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checks)
    result = checks.compare_traces(selected['smaller']['directory'], selected['larger']['directory'])
    return dict(status='passed', sources=sources,
                producers=result['producers'],
                interpretation='The logged evaluation partition sequences share identical prefixes despite different warm-up histories. Exact arrival timestamps and total counts are still reported separately.')


def verify_preparation(report):
    if report.get('status') != 'passed':
        raise ValueError('Preparation trace check did not pass')
    for source in report['sources'].values():
        directory = Path(source['directory'])
        expected = dict(source['event_sha256'], **{'manifest.json': source['manifest_sha256'],
                                                 'outcome-summary.json': source['outcome_sha256']})
        for name, sha in expected.items():
            if digest(directory / name) != sha:
                raise ValueError('Preparation evidence changed: ' + str(directory / name))
