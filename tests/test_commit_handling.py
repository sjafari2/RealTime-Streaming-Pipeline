"""Exercise group transitions without hiding commit failures or committing unprocessed work."""
import threading
from types import SimpleNamespace
from unittest.mock import Mock

from confluent_kafka import KafkaError, KafkaException, TopicPartition
import pytest

from test_measurements import consumer as module

@pytest.fixture
def worker():
    obj = module.MetricConsumer.__new__(module.MetricConsumer)
    obj.consumer = Mock()
    obj.consumer.commit.return_value = []
    obj.runtime = SimpleNamespace(event=Mock(), outcome=Mock(), failure=None,
                                  stop_event=threading.Event(), run_id='test')
    obj.labels = {k: 'commit-test' for k in module.LABELS}
    obj.assignments = {('t', 0): TopicPartition('t', 0)}
    obj.frontiers = {('t', 0): 20}
    obj.completed_offsets = {}
    return obj


def test_startup_assignment_is_not_completed_work(worker):
    worker.commit(asynchronous=True)
    worker.commit()
    worker.consumer.commit.assert_not_called()
    worker.completed_offsets[('t', 0)] = 21
    worker.commit(asynchronous=True)
    tp = worker.consumer.commit.call_args.kwargs['offsets'][0]
    assert tp.offset == 21


def test_stale_async_generation_is_recorded_then_current_progress_is_retried(worker):
    worker.completed_offsets[('t', 0)] = 21
    worker.on_commit(KafkaError(KafkaError.ILLEGAL_GENERATION), [TopicPartition('t', 0, 20)])
    assert worker.runtime.failure is None and not worker.runtime.stop_event.is_set()
    assert worker.runtime.event.call_args.kwargs['group_transition'] is True
    worker.commit(asynchronous=True)
    assert worker.consumer.commit.call_args.kwargs['offsets'][0].offset == 21
    worker.assignments.clear()
    worker.consumer.commit.reset_mock()
    worker.commit(asynchronous=True)
    worker.consumer.commit.assert_not_called()


def test_per_partition_error_is_checked_even_without_global_error(worker):
    tp = SimpleNamespace(topic='t', partition=0, offset=21,
                         error=KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED))
    worker.on_commit(None, [tp])
    assert worker.runtime.failure and worker.runtime.stop_event.is_set()
    assert worker.runtime.event.call_args.kwargs['group_transition'] is False


def test_revoke_continues_after_transition_error_without_retrying_old_owner(worker):
    worker.completed_offsets[('t', 0)] = 21
    worker.consumer.commit.side_effect = KafkaException(KafkaError(KafkaError.ILLEGAL_GENERATION))
    worker.forget = Mock(side_effect=lambda _: (worker.assignments.clear(), worker.completed_offsets.clear()))
    worker.ownership_event = Mock()
    partitions = [TopicPartition('t', 0)]
    worker.on_revoke(worker.consumer, partitions)
    worker.consumer.incremental_unassign.assert_called_once_with(partitions)
    assert worker.runtime.failure is None
    worker.consumer.commit.reset_mock()
    worker.commit()
    worker.consumer.commit.assert_not_called()


def test_unknown_synchronous_error_still_stops_the_process(worker):
    worker.completed_offsets[('t', 0)] = 21
    worker.consumer.commit.side_effect = KafkaException(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED))
    with pytest.raises(KafkaException):
        worker.commit()
    assert worker.runtime.failure and worker.runtime.stop_event.is_set()


def test_lost_assignment_does_not_commit(worker):
    worker.completed_offsets[('t', 0)] = 21
    worker.forget = Mock()
    worker.ownership_event = Mock()
    worker.on_lost(worker.consumer, [TopicPartition('t', 0)])
    worker.consumer.commit.assert_not_called()


def test_completed_message_updates_only_the_completion_frontier(worker):
    import time
    worker.epoch = 1
    worker.cpu_iterations = 0
    worker.delay = 0
    worker.deadline = .099
    msg = Mock()
    msg.headers.return_value = [('message_id', b'm1'), ('run_id', b'test'),
                                ('producer_timestamp', str(time.time() - .01).encode())]
    msg.value.return_value = b'payload'
    msg.topic.return_value = 't'
    msg.partition.return_value = 0
    msg.offset.return_value = 20
    worker.process_message(msg)
    assert worker.completed_offsets == {('t', 0): 21}
    assert worker.frontiers == {('t', 0): 21}
    worker.consumer.store_offsets.assert_called_once_with(message=msg)
    worker.runtime.outcome.assert_called_once()
