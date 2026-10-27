import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('verify_workload', ROOT / 'experiments/backlog-comparison/verify_workload.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def evidence(directory, partitions, indices=None):
    directory.mkdir()
    (directory / 'manifest.json').write_text(json.dumps(dict(evaluation_start_epoch=60,
        producer_end_epoch=180, producer_pods=['producer-workers-0'])))
    events = directory / 'producer/producer-workers-0/process/events.jsonl'
    events.parent.mkdir(parents=True)
    if indices is None:
        indices = range(len(partitions))
    events.write_text(''.join(json.dumps(dict(event='acknowledged', producer_timestamp=61+i*.01,
        workload_phase=1, workload_phase_index=i, partition=p))+'\n' for i, p in zip(indices, partitions)))
    return directory


def test_trace_equality_allows_one_extra_final_message(tmp_path):
    left = evidence(tmp_path / 'small', [1, 2, 3])
    right = evidence(tmp_path / 'large', [1, 2, 3, 4])
    result = module.compare_traces(left, right)
    assert result['status'] == 'passed'
    assert result['producers']['producer-workers-0']['compared_messages'] == 3


def test_different_partition_sequence_is_rejected(tmp_path):
    left = evidence(tmp_path / 'small', [1, 2, 3])
    right = evidence(tmp_path / 'large', [1, 3, 2])
    with pytest.raises(ValueError, match='sequence depends'):
        module.compare_traces(left, right)


def test_missing_acknowledgment_is_rejected(tmp_path):
    directory = evidence(tmp_path / 'missing', [1, 3], [0, 2])
    with pytest.raises(ValueError, match='missing acknowledgments'):
        module.evaluation_traces(directory)
