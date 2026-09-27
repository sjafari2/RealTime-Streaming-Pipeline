"""Scheduled replica addition followed by a verified explicit ownership handoff.

This pilot changes capacity and assignment together. It is not Kafka's automatic
assignor or an adaptive policy. New replicas remain empty until all owners release.
"""
import time
from explicit_control import transfer


def scale_and_transfer(api, control, startup_timeout=90, handoff_timeout=60):
    plan = control['intervention']
    original = control['consumer_pods']
    names = control['explicit_scale_consumers']
    identities = control['explicit_start_verification']['incarnations']
    deadline = time.time() + startup_timeout
    if startup_timeout <= 0 or deadline + handoff_timeout >= control['producer_end_epoch']:
        raise ValueError('Insufficient production time for enrollment and handoff')

    def current():
        value = api.read_control()
        if (not value or value.get('state') != 'running' or
                any(value.get(k) != control.get(k) for k in
                    ('run_id', 'config_sha256', 'consumer_pods', 'explicit_scale_consumers'))):
            raise RuntimeError('Managed run changed during explicit scale-up')
        return value

    try:
        value = current()
        if api.pods('consumer') != original or value.get('explicit_enrollment') or value.get('explicit_handoff'):
            raise RuntimeError('Scale-up requires unchanged initial membership and no earlier handoff')
        value['explicit_enrollment'] = dict(run_id=control['run_id'], members=names, deadline_epoch=deadline)
        api.publish(value)
        api.kubectl('scale', 'statefulset', 'consumer-sts',
                    '--current-replicas=' + str(plan['initial_consumers']),
                    '--replicas=' + str(plan['target_consumers']))
        api.journal(control, 'intervention-events.jsonl', dict(event='replicas_requested', timestamp=time.time(), members=names))
        while time.time() < deadline:
            current()
            present = api.pods('consumer')
            if not set(original) <= set(present) <= set(names):
                raise RuntimeError('Unexpected consumer membership during enrollment')
            api.resource_snapshot(control)
            ready = present == names
            for name in present:
                row = api.status('consumer', name)
                matches = (row.get('run_id') == control['run_id'] and
                           row.get('config_sha256') == control['config_sha256'])
                if matches and (row.get('failure') or row.get('evidence_error') or row.get('evidence_dropped')):
                    raise RuntimeError('Consumer failed during enrollment: ' + name)
                healthy = (matches and row.get('pod') == name and row.get('incarnation') and
                           row.get('assignment_mode') == 'explicit' and row.get('phase') in ('ready', 'running') and
                           time.time()-row.get('timestamp', 0) <= 10 and
                           row.get('explicit_assignment', {}).get('stage') == 'active' and
                           row.get('explicit_assignment', {}).get('epoch') == 0)
                if name in original:
                    if not healthy or row['incarnation'] != identities[name]:
                        raise RuntimeError('Original consumer identity or health changed: ' + name)
                else:
                    if healthy and row.get('assignments'):
                        raise RuntimeError('New consumer claimed partitions before the handoff')
                    ready = ready and bool(healthy)
            if ready:
                break
            time.sleep(.5)
        else:
            raise RuntimeError('New consumer enrollment timed out; no ownership handoff')
        # Check packages/shared source on the newly created pods as well.
        api.preflight()
        value = current()
        value['scale_clock_probes'] = api.clock_probes('consumer', [n for n in names if n not in original])
        api.publish(value)
        api.journal(control, 'intervention-events.jsonl', dict(event='empty_enrollment_verified', timestamp=time.time(), members=names))
        result = transfer(api, value, plan['target_assignment'], timeout=handoff_timeout)
        value = current()
        value['explicit_scale_result'] = result
        api.publish(value)
        return result
    except BaseException:
        value = api.read_control()
        if value and value.get('run_id') == control['run_id']:
            value['state'] = 'failed'
            api.publish(value)
        raise
