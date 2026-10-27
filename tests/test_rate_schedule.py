"""Check phase boundaries and pacing without changing Kafka or sleeping."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/common'))
from rate_schedule import RateSchedule


def schedule():
    return RateSchedule(json.dumps([
        {'at_seconds': 0, 'rate': 300},
        {'at_seconds': 60, 'rate': 200}]), 660)


def test_common_boundaries_and_no_time_based_interpolation():
    s = schedule()
    assert s.at(0) == (0, 300)
    assert s.at(59.999) == (0, 300)
    assert s.at(60) == (1, 200)
    assert s.at(659) == (1, 200)


@pytest.mark.parametrize('rows', [[], {}, [{'at_seconds': 1, 'rate': 10}],
    [{'at_seconds': 0, 'rate': 0}], [{'at_seconds': 0, 'rate': float('nan')}],
    [{'at_seconds': True, 'rate': 1}], [{'at_seconds': 0, 'rate': 1, 'extra': 2}],
    [{'at_seconds': 0, 'rate': 10}, {'at_seconds': 660, 'rate': 20}],
    [{'at_seconds': 0, 'rate': 10}, {'at_seconds': 0, 'rate': 20}]])
def test_bad_schedules_fail_before_traffic(rows):
    with pytest.raises(ValueError):
        RateSchedule(json.dumps(rows), 660)


def test_producer_uses_shared_start_and_clears_old_pacing_debt(monkeypatch):
    spec = importlib.util.spec_from_file_location('scheduled_producer', ROOT / 'src/producer/producer.py')
    module = importlib.util.module_from_spec(spec)
    # The main suite imports the producer too; use its existing metric objects.
    import prometheus_client
    monkeypatch.setattr(prometheus_client, 'Counter', lambda *a, **k: Mock())
    monkeypatch.setattr(prometheus_client, 'Gauge', lambda *a, **k: Mock())
    spec.loader.exec_module(module)
    p = module.MyProducer.__new__(module.MyProducer)
    p.rate_schedule, p.rate_stage, p.rate = schedule(), 0, 300
    p.labels = {'id': uuid.uuid4().hex}
    p.runtime = SimpleNamespace(control=lambda: {'start_epoch': 1000}, details={}, event=Mock())
    monkeypatch.setattr(module.time, 'time', lambda: 1060.25)
    assert p.update_rate(100, 99) == 100
    assert p.rate == 200 and p.rate_stage == 1
    event = p.runtime.event.call_args.kwargs
    assert event['scheduled_epoch'] == 1060
    assert event['scheduling_delay_seconds'] == .25
    assert event['discarded_pacing_debt'] == 300
    assert p.update_rate(100.01, 100.015) == 100.015
    p.runtime.event.assert_called_once()
    p.rate_schedule = None
    assert p.update_rate(101, 102) == 102
