"""Validate input-trace evidence and retain its hashes across resumed runs."""
import json
import sys
from pathlib import Path
import os

import pytest

sys.path.insert(0, str(Path(os.environ['PIPELINE_REPOSITORY']) / 'python-scripts'))
from backlog_evidence import check_preparation, verify_preparation


def controls(tmp_path):
    result = []
    pods = ['producer-workers-' + str(i) for i in range(3)]
    for condition in ('smaller', 'larger'):
        directory = tmp_path / condition
        directory.mkdir()
        (directory / 'manifest.json').write_text(json.dumps(dict(run_id=condition, producer_pods=pods,
            evaluation_start_epoch=60, producer_end_epoch=660)))
        (directory / 'outcome-summary.json').write_text(json.dumps(dict(validity_failures=[], admitted_messages_per_second=600)))
        for pod in pods:
            p = directory / 'producer' / pod / 'incarnation'
            p.mkdir(parents=True)
            (p / 'events.jsonl').write_text('\n'.join(json.dumps(dict(event='acknowledged',
                workload_phase=1, workload_phase_index=i, partition=i, producer_timestamp=61+i))
                for i in range(3))+'\n')
        result.append(dict(condition=condition, directory=str(directory), report={'status': 'passed'}))
    return result


def test_preparation_reuses_trace_checks_and_rejects_changed_evidence(tmp_path):
    items = controls(tmp_path)
    report = check_preparation(items)
    assert report['status'] == 'passed'
    verify_preparation(report)
    path = tmp_path / 'larger/producer/producer-workers-0/incarnation/events.jsonl'
    path.write_text(path.read_text().replace('"partition": 1', '"partition": 2'))
    with pytest.raises(ValueError, match='changed'):
        verify_preparation(report)
    with pytest.raises(ValueError, match='sequence'):
        check_preparation(items)


def test_underproduction_or_missing_control_cannot_pass_preparation(tmp_path):
    items = controls(tmp_path)
    with pytest.raises(ValueError, match='Both'):
        check_preparation(items[:1])
    path = tmp_path / 'smaller/outcome-summary.json'
    path.write_text(json.dumps(dict(validity_failures=[], admitted_messages_per_second=590)))
    with pytest.raises(ValueError, match='600'):
        check_preparation(items)
