"""Opt-in, fail-closed assignment adapter for a fixed-membership synthetic pilot.

The coordinator releases ALL consumers before acquiring a new map. This is an
application barrier, not Kafka group fencing or a fault-tolerant assignor.
"""
import json
import math
import os
import time


def ownership_map(rows, expected_partitions, consumers):
    if not isinstance(rows, list) or not rows:
        raise ValueError('Explicit assignment requires a complete partition map')
    result = {}
    for row in rows:
        p, owner = row.get('partition'), row.get('owner')
        if type(p) is not int or p < 0 or p in result or owner not in consumers:
            raise ValueError('Duplicate partition or unknown explicit owner')
        result[p] = owner
    if set(result) != set(range(expected_partitions)):
        raise ValueError('Explicit map must cover every partition once')
    return result


class ExplicitAssignment:
    def __init__(self, worker):
        self.worker = worker
        self.runtime = worker.runtime
        control = self.runtime.control()
        if not self.runtime.control_path or control['state'] != 'preparing':
            raise ValueError('Explicit consumers must start in a managed preparation')
        if len(worker.topics) != 1:
            raise ValueError('The explicit pilot supports exactly one topic')
        if str(os.getenv('CONSUMER_STATIC_MEMBERSHIP', 'false')).lower() != 'false':
            raise ValueError('Explicit assignment must not claim static group membership')
        self.consumers = tuple(control['consumer_pods'])
        self.count = int(os.environ['NUM_PARTITIONS'])
        self.initial = ownership_map(json.loads(os.environ['EXPLICIT_ASSIGNMENT_JSON']), self.count, self.consumers)
        self.epoch = 0
        self.stage = 'active'
        self.offsets = {}
        self.target = None
        self.transaction = None
        self.runtime.details['assignment_mode'] = 'explicit'
        self._status()

    def _status(self):
        self.runtime.details['explicit_assignment'] = dict(epoch=self.epoch, stage=self.stage,
            offsets={str(p): offset for p, offset in self.offsets.items()})

    def initialize(self):
        w = self.worker
        parts = [w.topic_partition(w.topics[0], p) for p, owner in sorted(self.initial.items())
                 if owner == self.runtime.pod]
        w.on_assign(w.consumer, parts)

    def check(self):
        """Called at batch boundaries on the processing thread, never from HTTP."""
        control = self.runtime.control()
        command = control.get('explicit_handoff')
        if command is None:
            if self.stage != 'active' or self.epoch:
                raise RuntimeError('An explicit handoff command disappeared')
            return True
        w = self.worker
        epoch = command.get('epoch')
        if type(epoch) is not int or epoch < 1:
            raise ValueError('Invalid handoff epoch')
        identities = command.get('incarnations', {})
        if (command.get('run_id') != self.runtime.run_id or set(identities) != set(self.consumers) or
                identities.get(self.runtime.pod) != self.runtime.incarnation):
            raise RuntimeError('Handoff run or process identity changed')
        deadline = command.get('deadline_epoch')
        if isinstance(deadline, bool) or not isinstance(deadline, (int,float)) or not math.isfinite(deadline):
            raise ValueError('A finite handoff deadline is required')
        target = ownership_map(command['ownership'], self.count, self.consumers)
        transaction = (epoch, tuple(sorted(identities.items())), tuple(sorted(target.items())), deadline)
        stage = command.get('stage')
        if stage == 'failed':
            raise RuntimeError('Coordinator aborted explicit handoff')
        if epoch == self.epoch and self.transaction != transaction:
            raise RuntimeError('Handoff identity, target or deadline changed within an epoch')
        if stage != 'resume' and time.time() >= deadline:
            raise RuntimeError('Explicit handoff deadline expired; no automatic takeover')
        if stage == 'release':
            if epoch == self.epoch and self.stage == 'released':
                return False
            if epoch != self.epoch + 1 or self.stage != 'active':
                raise RuntimeError('Unexpected release sequence')
            self.epoch, self.transaction, self.target = epoch, transaction, target
            # Full completed-prefix offsets include partitions with no new work.
            parts = list(w.assignments.values())
            self.offsets = {p: w.frontiers[(t,p)] for t,p in w.assignments}
            offsets = [w.topic_partition(w.topics[0],p,o) for p,o in sorted(self.offsets.items())]
            result = w.consumer.commit(offsets=offsets, asynchronous=False) if offsets else []
            if offsets and (not result or len(result) != len(offsets) or any(tp.error for tp in result)):
                raise RuntimeError('Explicit release commit was not acknowledged for every partition')
            actual = w.consumer.committed(offsets, timeout=5) if offsets else []
            if {(p.partition,p.offset) for p in actual} != set(self.offsets.items()) or any(p.error for p in actual):
                raise RuntimeError('Explicit release commit readback differs from completion')
            self.runtime.writer.flush(timeout=10)
            w.consumer.unassign()
            w.forget(parts)
            w.ownership_event('explicit_release', parts)
            self.stage = 'released'
            self._status()
            self.runtime.event('explicit_released', epoch=self.epoch, offsets=self.runtime.details['explicit_assignment']['offsets'])
            return False
        if stage == 'acquire':
            if epoch != self.epoch or self.stage not in ('released','acquired'):
                raise RuntimeError('Acquire requires release in the same epoch')
            expected = command.get('offsets', {})
            if set(expected) != {str(p) for p in range(self.count)} or any(type(o) is not int or o < 0 for o in expected.values()):
                raise ValueError('Acquire needs a complete verified offset map')
            if self.stage == 'acquired':
                if self.acquire_offsets != expected:
                    raise RuntimeError('Acquire offsets changed')
                return False
            self.acquire_offsets = dict(expected)
            parts = [w.topic_partition(w.topics[0],p) for p,owner in sorted(target.items()) if owner == self.runtime.pod]
            # on_assign reads committed offsets and retained bounds. Recheck the
            # coordinator's exact offsets before allowing any processing.
            w.on_assign(w.consumer, parts)
            if any(w.frontiers[(w.topics[0],p.partition)] != expected[str(p.partition)] for p in parts):
                raise RuntimeError('Destination offset differs from verified release')
            if parts:
                w.consumer.pause(parts)
            self.stage = 'acquired'
            self.offsets = {p.partition: expected[str(p.partition)] for p in parts}
            self._status()
            self.runtime.event('explicit_acquired', epoch=self.epoch, offsets=self.runtime.details['explicit_assignment']['offsets'])
            return False
        if stage == 'resume':
            if epoch != self.epoch or self.stage not in ('acquired','active'):
                raise RuntimeError('Resume requires acquisition in the same epoch')
            if command.get('offsets') != self.acquire_offsets:
                raise RuntimeError('Resume offsets changed')
            if self.stage == 'acquired':
                parts=list(w.assignments.values())
                if parts:
                    w.consumer.resume(parts)
                self.stage='active'
                self._status()
                self.runtime.event('explicit_resumed', epoch=self.epoch)
            return True
        raise ValueError('Unknown explicit handoff stage')
