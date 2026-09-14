"""Offline validator for the planned release-before-acquire handoff protocol.

This state machine checks evidence contracts. It neither fences a live consumer
nor calls Kafka. A future adapter must establish each fact it submits here.
"""


class InvalidHandoff(ValueError):
    pass


class Handoff:
    def __init__(self, run_id, epoch, ownership, moves, incarnations):
        if not run_id or type(epoch) is not int or epoch < 0:
            raise InvalidHandoff('A run and nonnegative epoch are required')
        if not ownership or any(type(p) is not int or p < 0 for p in ownership):
            raise InvalidHandoff('Invalid ownership map')
        if not incarnations or any(not isinstance(v, str) or not v for v in incarnations.values()):
            raise InvalidHandoff('Exact process incarnations are required')
        if set(ownership.values()) - set(incarnations):
            raise InvalidHandoff('Unknown initial owner')
        self.run_id, self.epoch = run_id, epoch
        self.ownership, self.incarnations = dict(ownership), dict(incarnations)
        self.moves = {}
        for move in moves:
            p, source, dest = move['partition'], move['source'], move['destination']
            if (type(p) is not int or p in self.moves or ownership.get(p) != source or
                    dest not in incarnations or dest == source):
                raise InvalidHandoff('Duplicate move, wrong source or unknown destination')
            self.moves[p] = dict(move)
        if not self.moves:
            raise InvalidHandoff('A no-action decision must not start a handoff')
        self.released, self.acquired = {}, set()
        self.phase = 'releasing'
        self.failure = None

    def _identity(self, row, owner):
        if (self.phase == 'failed' or row.get('run_id') != self.run_id or
                type(row.get('epoch')) is not int or row.get('epoch') != self.epoch or row.get('owner') != owner or
                row.get('incarnation') != self.incarnations[owner]):
            raise InvalidHandoff('Stale run, epoch, process or failed handoff')

    def release(self, row):
        p = row.get('partition')
        if self.phase != 'releasing' or p not in self.moves or p in self.released:
            raise InvalidHandoff('Unexpected or duplicate release')
        self._identity(row, self.moves[p]['source'])
        if not all(row.get(k) is True for k in ('processing_stopped', 'unassigned', 'commit_verified', 'evidence_flushed')):
            raise InvalidHandoff('Release requires quiescence, unassignment, verified commit and durable evidence')
        offset = row.get('next_offset')
        if type(offset) is not int or offset < 0 or type(row.get('committed_offset')) is not int or row.get('committed_offset') != offset:
            raise InvalidHandoff('Resume offset must equal verified completed progress')
        self.released[p] = offset
        self.ownership[p] = None
        if len(self.released) == len(self.moves):
            self.phase = 'acquiring'

    def acquire(self, row):
        p = row.get('partition')
        if self.phase != 'acquiring' or p not in self.moves or p in self.acquired:
            raise InvalidHandoff('All releases must be verified before any destination acquires')
        self._identity(row, self.moves[p]['destination'])
        if row.get('processing_paused') is not True or type(row.get('next_offset')) is not int or row.get('next_offset') != self.released[p]:
            raise InvalidHandoff('Destination must remain paused at the verified resume offset')
        self.acquired.add(p)
        self.ownership[p] = self.moves[p]['destination']
        if len(self.acquired) == len(self.moves):
            self.phase = 'ready_to_resume'

    def resume(self, run_id, epoch, observed_ownership, observed_incarnations):
        if (self.phase != 'ready_to_resume' or run_id != self.run_id or epoch != self.epoch or
                observed_ownership != self.ownership or observed_incarnations != self.incarnations):
            raise InvalidHandoff('Resume needs complete matching ownership and unchanged processes')
        self.phase = 'resumed'

    def abort(self, reason):
        # A timeout never authorizes takeover. Stop and preserve evidence; do not
        # guess whether an unresponsive old owner has ceased processing.
        self.failure = str(reason)
        self.phase = 'failed'
