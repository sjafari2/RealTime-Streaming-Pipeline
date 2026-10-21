import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[1]))
SCRIPT = Path(os.environ.get('ACTION_WINDOWS_SCRIPT', ROOT/'experiments/four-condition-20260929/analyze_action_windows.py'))
spec = importlib.util.spec_from_file_location('action_windows', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_completion_window_boundaries_warmup_and_pending():
    ack = {'warm': (90, 91), 'at_left': (110, 111), 'middle': (130, 131),
           'at_right': (140, 141), 'missing': (150, 151), 'not_yet_ack': (155, 161)}
    done = {'warm': (135, 90, 'x'), 'at_left': (130, 110, 'x'),
            'middle': (155, 130, 'x'), 'at_right': (160, 140, 'x')}
    series = m.build_series(ack, done, 100, 700)
    window = m.window_metrics(series, 130, 160)
    assert window['distinct_completions'] == 3
    assert window['completion_throughput_per_second'] == .1
    assert window['evaluation_completions_for_latency'] == 2
    assert window['completion_window_p99_seconds'] == 25
    assert window['completion_window_mean_seconds'] == 22.5
    assert window['acknowledged_evaluation_messages_pending_at_end'] == 2


def test_produced_before_action_but_completed_after_is_not_pre_action_latency():
    series = m.build_series({'m': (145, 146)}, {'m': (175, 145, 'x')}, 100, 700)
    before = m.window_metrics(series, 130, 160)
    after = m.window_metrics(series, 160, 190)
    assert before['completion_window_p99_seconds'] is None
    assert before['acknowledged_evaluation_messages_pending_at_end'] == 1
    assert after['completion_window_p99_seconds'] == 30


def test_nearest_rank_uses_messages_and_empty_stays_unavailable():
    assert m.p99(range(1, 101)) == 99
    assert m.p99([1]*99 + [1000]) == 1
    assert m.p99([]) is None


@pytest.mark.parametrize('left,right', [(1,1),(2,1),(0,float('inf')),(float('nan'),2)])
def test_invalid_windows_rejected(left, right):
    with pytest.raises(ValueError, match='positive finite'):
        m.window_metrics({}, left, right)


def test_clock_order_does_not_make_pending_count_negative():
    series = m.build_series({'m': (100, 120)}, {'m': (110, 100, 'x')}, 100, 700)
    assert m.window_metrics(series, 100, 115)['acknowledged_evaluation_messages_pending_at_end'] == 0
    assert m.window_metrics(series, 100, 121)['acknowledged_evaluation_messages_pending_at_end'] == 0
