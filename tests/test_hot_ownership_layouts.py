"""Check trial selection and preserve the workload while changing its owners."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / 'experiments/hot-ownership-20260927/run.py'
spec = importlib.util.spec_from_file_location('hot_ownership_runner', PATH)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def test_consumer2_selection_does_not_rerun_other_layouts():
    assert [name for name, _ in runner.configs(('concentrated-c2',))] == ['concentrated-c2']
    output = subprocess.check_output([sys.executable, str(PATH), '--layouts', 'concentrated-c2'])
    design = json.loads(output)
    assert [row['layout'] for row in design['runs']] == ['concentrated-c2']
    assert design['runs'][0]['hot_partition_counts'] == [0, 0, 12]


def test_original_default_selection_is_preserved():
    assert [name for name, _ in runner.configs()] == ['distributed', 'concentrated']


@pytest.mark.parametrize('layout,owner', [('concentrated-c2',2), ('concentrated-c1',1)])
def test_concentration_changes_only_identity_and_ownership(layout,owner):
    baseline = runner.configs(('distributed',))[0][1]['data']
    target = runner.configs((layout,))[0][1]['data']
    assert baseline.keys() == target.keys()
    assert {k for k in baseline if baseline[k] != target[k]} == {
        'EXP_ID', 'CONSUMER_GROUP_ID', 'EXPLICIT_ASSIGNMENT_JSON'}
    owners = json.loads(target['EXPLICIT_ASSIGNMENT_JSON'])
    assert len(owners) == 60 and {x['partition'] for x in owners} == set(range(60))
    for i in range(3):
        partitions = {x['partition'] for x in owners if x['owner'] == f'consumer-sts-{i}'}
        assert len(partitions) == 20
        assert len(partitions & set(range(12))) == (12 if i == owner else 0)


def test_duplicate_selection_cannot_launch_a_repeat():
    result = subprocess.run([sys.executable, str(PATH), '--layouts', 'concentrated-c2', 'concentrated-c2'],
                            capture_output=True, text=True)
    assert result.returncode != 0 and 'List each layout once' in result.stderr


def test_followup_order_is_consumer2_then_consumer1_only():
    output = subprocess.check_output([sys.executable, str(PATH), '--layouts', 'concentrated-c2', 'concentrated-c1'])
    design = json.loads(output)
    assert [row['layout'] for row in design['runs']] == ['concentrated-c2', 'concentrated-c1']
    assert [row['hot_partition_counts'] for row in design['runs']] == [[0,0,12],[0,12,0]]
