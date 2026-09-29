"""An assigned offset stays known before the first record, without hiding failures."""
import math
from types import SimpleNamespace
import pytest
from test_measurements import app, consumer, Message


def unresolved(app, monkeypatch):
    monkeypatch.setattr(app.consumer, 'position', lambda parts: [
        consumer.TopicPartition(p.topic, p.partition, consumer.OFFSET_INVALID) for p in parts])


def test_nonempty_partition_is_measurable_before_first_record(app, monkeypatch):
    key=('topic_0', 0)
    app.initial_positions[key]=17
    app.frontiers[key]=17
    unresolved(app, monkeypatch)
    app.update_lag()
    labels=app.partition_labels(key)
    assert consumer.position_metric.labels(**labels)._value.get()==17
    assert consumer.raw_position_metric.labels(**labels)._value.get()==consumer.OFFSET_INVALID
    assert consumer.lag_metric.labels(**labels)._value.get()==83
    assert consumer.backlog_metric.labels(**labels)._value.get()==83
    assert consumer.initial_position_metric.labels(**labels)._value.get()==1
    assert not app.completed_offsets  # Measurement does not invent completion or commit progress.
    assert app.runtime.rows[-1]['source']=='assignment_start'


def test_entire_returned_batch_disables_assignment_fallback(app, monkeypatch):
    unresolved(app, monkeypatch)
    app.record_returned([Message(0), Message(1)])
    assert ('topic_0',0) not in app.initial_positions
    app.update_lag()
    labels=app.partition_labels(('topic_0',0))
    assert consumer.valid_metric.labels(**labels)._value.get()==0
    assert math.isnan(consumer.lag_metric.labels(**labels)._value.get())
    row=app.runtime.rows[-1]
    assert row['raw_position']==consumer.OFFSET_INVALID and row['low']==0 and row['high']==100


def test_native_position_disables_fallback_even_without_record_callback(app, monkeypatch):
    app.update_lag()
    assert not app.initial_positions
    app.observations.clear()
    unresolved(app, monkeypatch)
    app.update_lag()
    assert not app.observations


@pytest.mark.parametrize('low, high, raw', [(18,100,-1001),(0,16,-1001),(0,100,-2),(0,100,101)])
def test_retention_truncation_and_other_invalid_offsets_stay_invalid(app, monkeypatch, low, high, raw):
    app.initial_positions[('topic_0',0)]=17
    app.frontiers[('topic_0',0)]=17
    monkeypatch.setattr(app.consumer,'position',lambda parts:[consumer.TopicPartition('topic_0',0,raw)])
    monkeypatch.setattr(app.consumer,'get_watermark_offsets',lambda *a,**k:(low,high))
    app.update_lag()
    assert not app.observations


def test_query_failure_never_uses_old_bounds(app, monkeypatch):
    unresolved(app, monkeypatch)
    monkeypatch.setattr(app.consumer,'get_watermark_offsets',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('timeout')))
    app.update_lag()
    assert not app.observations
    assert app.runtime.rows[-1]['reason']=='query_failed'


def test_revoke_clears_start_and_reassignment_initializes_new_offset(app, monkeypatch):
    key=('topic_0',0)
    app.forget(list(app.assignments.values()))
    assert not app.initial_positions
    monkeypatch.setattr(app.consumer,'committed',lambda parts,timeout:[consumer.TopicPartition('topic_0',0,42)])
    app.on_assign(app.consumer,[consumer.TopicPartition('topic_0',0)])
    assert app.initial_positions=={key:42}
    unresolved(app, monkeypatch)
    app.update_lag()
    assert app.observations[key]['lag']==58


def test_assignment_based_observation_expires_normally(app, monkeypatch):
    clock=SimpleNamespace(now=100.)
    monkeypatch.setattr(consumer,'time',SimpleNamespace(time=lambda:clock.now,monotonic=lambda:clock.now))
    unresolved(app, monkeypatch)
    app.update_lag()
    clock.now=111.
    app.refresh_lag_validity()
    labels=app.partition_labels(('topic_0',0))
    assert consumer.valid_metric.labels(**labels)._value.get()==0
    assert math.isnan(consumer.lag_metric.labels(**labels)._value.get())
