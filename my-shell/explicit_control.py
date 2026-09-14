"""Fixed-membership, full-consumer barrier for the explicit-assignment pilot."""
import copy
import time
from explicit_assignment import ownership_map


def transfer(api, control, target_rows, timeout=60):
    names = control['consumer_pods']
    count = int(control['config']['NUM_PARTITIONS'])
    target = ownership_map(target_rows, count, names)
    topic = control['config']['TOPIC_TITLE'] + '_0'
    deadline = time.time() + timeout
    if timeout <= 0 or deadline >= control['producer_end_epoch']:
        raise ValueError('Handoff needs a positive deadline inside the production interval')

    def current():
        value = api.read_control()
        if not value or any(value.get(k) != control.get(k) for k in ('run_id', 'config_sha256')) or value.get('state') != 'running':
            raise RuntimeError('Managed run changed during handoff')
        if api.pods('consumer') != names:
            raise RuntimeError('Consumer membership changed during handoff')
        return value

    def statuses():
        rows = {name: api.status('consumer', name) for name in names}
        for name, row in rows.items():
            if (any(row.get(k) != control.get(k) for k in ('run_id', 'config_sha256')) or
                    row.get('pod') != name or not row.get('incarnation') or
                    row.get('assignment_mode') != 'explicit' or row.get('phase') not in ('ready', 'running') or
                    time.time() - row.get('timestamp', 0) > 10 or
                    row.get('failure') or row.get('evidence_error') or row.get('evidence_dropped')):
                raise RuntimeError('Unhealthy or stale explicit consumer: ' + name)
        return rows

    def assignment(rows):
        owners = []
        for name, row in rows.items():
            for part in row.get('assignments', []):
                if part[0] != topic:
                    raise RuntimeError('Unexpected assignment topic')
                owners.append(dict(partition=part[1], owner=name))
        return ownership_map(owners, count, names)

    current()
    initial = statuses()
    source = assignment(initial)
    if any(row.get('explicit_assignment', {}).get('stage') != 'active' for row in initial.values()):
        raise RuntimeError('Consumers are not all active')
    if source == target:
        api.journal(control, 'explicit-handoff-events.jsonl', dict(event='no_assignment_change', timestamp=time.time()))
        return {'status': 'no_action'}
    epochs = {row['explicit_assignment']['epoch'] for row in initial.values()}
    if len(epochs) != 1:
        raise RuntimeError('Consumers disagree on the assignment epoch')
    epoch = epochs.pop() + 1
    identities = {name: row['incarnation'] for name, row in initial.items()}
    command = dict(run_id=control['run_id'], epoch=epoch, incarnations=identities,
                   ownership=target_rows, deadline_epoch=deadline)

    def send(stage):
        value = current()
        command['stage'] = stage
        value['explicit_handoff'] = copy.deepcopy(command)
        api.publish(value)
        api.journal(control, 'explicit-handoff-events.jsonl', dict(event=stage + '_requested', timestamp=time.time(), command=copy.deepcopy(command)))

    def await_stage(stage):
        while time.time() < deadline:
            current()
            rows = statuses()
            if any(rows[name]['incarnation'] != identities[name] for name in names):
                raise RuntimeError('A consumer process restarted during handoff')
            if all(row.get('explicit_assignment', {}).get('epoch') == epoch and
                   row['explicit_assignment']['stage'] == stage for row in rows.values()):
                api.journal(control, 'explicit-handoff-events.jsonl', dict(event=stage + '_verified', timestamp=time.time(), statuses=rows))
                return rows
            time.sleep(0.2)
        raise RuntimeError('Handoff timed out; no automatic takeover')

    try:
        send('release')
        released = await_stage('released')
        offsets = {}
        for name, row in released.items():
            if row.get('assignments'):
                raise RuntimeError('An old owner has not released its partitions')
            for part, offset in row['explicit_assignment']['offsets'].items():
                if source.get(int(part)) != name or part in offsets or type(offset) is not int or offset < 0:
                    raise RuntimeError('Invalid completion frontier from old owner')
                offsets[part] = offset
        if set(offsets) != {str(p) for p in range(count)}:
            raise RuntimeError('Release did not cover every partition')
        command['offsets'] = offsets
        send('acquire')
        acquired = await_stage('acquired')
        if assignment(acquired) != target:
            raise RuntimeError('Acquired ownership differs from target')
        for name, row in acquired.items():
            expected = {str(p): offsets[str(p)] for p, owner in target.items() if owner == name}
            if row['explicit_assignment']['offsets'] != expected:
                raise RuntimeError('Acquired offsets differ from verified release')
        send('resume')
        await_stage('active')
        return dict(status='resumed', epoch=epoch, offsets=offsets)
    except BaseException:
        value = api.read_control()
        if value and value.get('run_id') == control['run_id']:
            command['stage'] = 'failed'
            value['explicit_handoff'] = copy.deepcopy(command)
            value['state'] = 'failed'
            api.publish(value)
        raise
