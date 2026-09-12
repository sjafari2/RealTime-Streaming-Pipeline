"""Check a frozen starting assignment and pod placement before releasing a run."""
import copy
import hashlib
import json


class PlacementMismatch(RuntimeError):
    """The observed starting condition is not the one approved for this block."""


def reference_hash(reference):
    return hashlib.sha256(json.dumps(reference, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def validate_reference(reference, config=None):
    if not isinstance(reference, dict) or reference.get('schema_version') != 1:
        raise ValueError('Placement reference requires schema_version=1')
    assignments, pods = reference.get('assignment'), reference.get('pods')
    if not isinstance(assignments, list) or not assignments or not isinstance(pods, list) or not pods:
        raise ValueError('Placement reference needs complete assignment and pod records')
    names = set()
    consumers = set()
    for pod in pods:
        if pod.get('role') not in ('producer', 'consumer'):
            raise ValueError('Unknown role in placement reference')
        for field in ('pod', 'uid', 'node', 'container_id', 'image_id'):
            if not isinstance(pod.get(field), str) or not pod[field]:
                raise ValueError('Placement reference is missing ' + field)
        if pod['pod'] in names:
            raise ValueError('Duplicate pod in placement reference')
        names.add(pod['pod'])
        if pod['role'] == 'consumer':
            consumers.add(pod['pod'])
        if not isinstance(pod.get('resources'), dict) or not pod['resources'].get('requests'):
            raise ValueError('Placement reference needs declared container resources')
        if type(pod.get('restart_count')) is not int or pod['restart_count'] < 0:
            raise ValueError('Placement reference needs a valid restart count')
    if not consumers or not any(p['role'] == 'producer' for p in pods):
        raise ValueError('Placement reference needs both producers and consumers')
    seen = set()
    for row in assignments:
        if any(type(row.get(k)) is not int or row[k] < 0 for k in ('topic_index', 'partition')):
            raise ValueError('Invalid topic index or partition in placement reference')
        key = (row['topic_index'], row['partition'])
        if key in seen or row.get('pod') not in consumers:
            raise ValueError('Duplicate partition or unknown owner in placement reference')
        seen.add(key)
    if config is not None:
        expected = {(t, p) for t in range(int(config['TOPIC_COUNT']))
                    for p in range(int(config['NUM_PARTITIONS']))}
        if seen != expected:
            raise ValueError('Placement reference must cover every configured partition exactly once')
        for role in ('producer', 'consumer'):
            if sum(p['role'] == role for p in pods) != int(config[role.upper() + '_POD_COUNT']):
                raise ValueError('Placement reference replica count differs from configuration')
    return reference


def pod_identity(item):
    """Include pod and container identity: the same pod name can hide a restart."""
    meta, spec, state = (item.get(k, {}) for k in ('metadata', 'spec', 'status'))
    role = {'producer-sts': 'producer', 'consumer-sts': 'consumer'}.get(meta.get('labels', {}).get('app'))
    if role is None:
        raise PlacementMismatch('Unexpected pod in placement observation')
    name = meta.get('name', '?')
    container_name = role + '-container'
    container = next((c for c in spec.get('containers', []) if c['name'] == container_name), None)
    status = next((c for c in state.get('containerStatuses', []) if c['name'] == container_name), None)
    ready = any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in state.get('conditions', []))
    if (meta.get('deletionTimestamp') or state.get('phase') != 'Running' or not ready or
            container is None or status is None or not status.get('ready') or
            'running' not in status.get('state', {})):
        raise PlacementMismatch(name + ': pod/container is not continuously ready')
    return dict(role=role, pod=name, uid=meta.get('uid'), node=spec.get('nodeName'),
                container_id=status.get('containerID'), image_id=status.get('imageID'),
                restart_count=status.get('restartCount'), resources=copy.deepcopy(container.get('resources', {})))


def check_placement(reference, config, run_id, config_hash, status_rows, pod_items,
                    expected_processes=None, preparing=False):
    validate_reference(reference, config)
    current_pods = [pod_identity(item) for item in pod_items]
    by_name = {p['pod']: p for p in current_pods}
    expected_pods = {p['pod']: p for p in reference['pods']}
    if len(by_name) != len(current_pods) or set(by_name) != set(expected_pods):
        raise PlacementMismatch('Observed producer/consumer pod set differs from the block reference')
    for name, expected in expected_pods.items():
        if by_name[name] != expected:
            fields = [key for key in expected if by_name[name].get(key) != expected[key]]
            raise PlacementMismatch(name + ': changed ' + ', '.join(fields))
    if set(status_rows) != set(expected_pods):
        raise PlacementMismatch('Missing application status for a reference pod')
    actual, processes = [], {}
    topics = {f"{config['TOPIC_TITLE']}_{i}": i for i in range(int(config['TOPIC_COUNT']))}
    for name, row in status_rows.items():
        role = expected_pods[name]['role']
        if (row.get('pod') != name or row.get('role') != role or row.get('run_id') != run_id or
                row.get('config_sha256') != config_hash or row.get('failure') or row.get('evidence_error') or
                row.get('evidence_dropped', 0) or not row.get('incarnation')):
            raise PlacementMismatch(name + ': missing, failed, or different-run application status')
        if row.get('phase') not in (('ready',) if preparing else ('ready', 'running')):
            raise PlacementMismatch(name + ': unexpected application phase')
        processes[name] = {'incarnation': row['incarnation']}
        if role == 'consumer':
            if type(row.get('assignment_epoch')) is not int:
                raise PlacementMismatch(name + ': missing assignment epoch')
            processes[name]['assignment_epoch'] = row['assignment_epoch']
            for topic, partition in row.get('assignments', []):
                if topic not in topics or type(partition) is not int:
                    raise PlacementMismatch(name + ': unexpected topic or partition')
                actual.append(dict(topic_index=topics[topic], partition=partition, pod=name))
    order = lambda row: (row['topic_index'], row['partition'], row['pod'])
    if sorted(actual, key=order) != sorted(reference['assignment'], key=order):
        raise PlacementMismatch('Complete partition ownership differs from the block reference')
    if expected_processes is not None and processes != expected_processes:
        raise PlacementMismatch('Application incarnation or assignment epoch changed before intervention')
    return processes
