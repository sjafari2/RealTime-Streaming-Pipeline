"""An exact ownership check must reject full coverage with the wrong owners."""
import copy
import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'my-shell'))
from run_experiment import verify_explicit_start


def sample():
    config = dict(CONSUMER_ASSIGNMENT_MODE='explicit', NUM_PARTITIONS='2', TOPIC_TITLE='topic',
                  EXPLICIT_ASSIGNMENT_JSON=json.dumps([dict(partition=0, owner='c0'), dict(partition=1, owner='c1')]))
    rows = [dict(pod='c'+str(i), incarnation='i'+str(i), run_id='r', phase='ready',
                 assignment_mode='explicit', assignments=[['topic_0', i]],
                 explicit_assignment=dict(epoch=0, stage='active')) for i in range(2)]
    return config, rows


def test_verified_map_records_process_identities():
    config, rows = sample()
    before = copy.deepcopy(rows)
    result = verify_explicit_start(rows, ['c0','c1'], config, 'r')
    assert result['verified'] and result['incarnations'] == {'c0':'i0', 'c1':'i1'}
    assert rows == before


@pytest.mark.parametrize('fault', ['swapped', 'duplicate', 'wrong_topic', 'wrong_run', 'missing', 'mode', 'epoch', 'incarnation'])
def test_no_traffic_when_exact_ownership_is_unverified(fault):
    config, rows = sample()
    if fault == 'swapped': rows[0]['assignments'], rows[1]['assignments'] = rows[1]['assignments'], rows[0]['assignments']
    if fault == 'duplicate': rows[0]['assignments'] *= 2
    if fault == 'wrong_topic': rows[0]['assignments'][0][0] = 'old_0'
    if fault == 'wrong_run': rows[0]['run_id'] = 'old'
    if fault == 'missing': rows.pop()
    if fault == 'mode': rows[0]['assignment_mode'] = 'cooperative'
    if fault == 'epoch': rows[0]['explicit_assignment']['epoch'] = 1
    if fault == 'incarnation': rows[0]['incarnation'] = ''
    with pytest.raises(RuntimeError, match='no workload released'):
        verify_explicit_start(rows, ['c0','c1'], config, 'r')


def test_cooperative_runs_do_not_require_an_explicit_map():
    assert verify_explicit_start([], [], {}, 'r') is None
