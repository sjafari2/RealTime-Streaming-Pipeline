"""A lossless representation must preserve research results and fail visibly on damage."""
import gzip
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'python-scripts'))
from analyze_execution import analyze
from compress_evidence import compress
from evaluate_run import evaluate
from evidence_io import event_paths
from test_analysis import make_run
from test_research_metrics import metadata


def completed_run(path):
    directory = metadata(make_run(path, unfinished=True, duplicate=True))
    (directory / 'runner-status.json').write_text(json.dumps(dict(run_id='r', status='complete')))
    return directory


def test_lossless_evidence_preserves_outcomes_and_execution(tmp_path):
    directory = completed_run(tmp_path / 'run')
    outcome_before = evaluate(directory)
    execution_before = analyze(directory)
    originals = {str(p.relative_to(directory)): p.read_bytes() for p in event_paths(directory)}
    report = compress(directory)
    assert len(report['files']) == 2
    for row in report['files']:
        assert gzip.decompress((directory / row['compressed_path']).read_bytes()) == originals[row['original_path']]
        assert not (directory / row['original_path']).exists()
    assert evaluate(directory) == outcome_before
    execution_after = analyze(directory)
    # Only the evidence's filename changes; measured lifetimes and events do not.
    for row in execution_after['process_lifetimes']:
        row['evidence'] = row['evidence'].removesuffix('.gz')
    assert execution_after == execution_before
    assert compress(directory) == report


def test_duplicate_or_corrupt_representation_is_not_silently_counted(tmp_path):
    directory = completed_run(tmp_path / 'run')
    original = directory / 'consumer/events.jsonl'
    data = original.read_bytes()
    compressed = original.with_name('events.jsonl.gz')
    compressed.write_bytes(gzip.compress(data))
    with pytest.raises(ValueError, match='Both plain and compressed'):
        evaluate(directory)
    original.unlink()
    compressed.write_bytes(b'not gzip')
    with pytest.raises(gzip.BadGzipFile):
        evaluate(directory)


def test_incomplete_run_is_preserved_without_compression(tmp_path):
    directory = completed_run(tmp_path / 'run')
    (directory / 'runner-status.json').write_text(json.dumps(dict(run_id='r', status='failed')))
    with pytest.raises(ValueError, match='completed, validated'):
        compress(directory)
    assert len(event_paths(directory)) == 2
    assert not list(directory.rglob('*.gz'))
