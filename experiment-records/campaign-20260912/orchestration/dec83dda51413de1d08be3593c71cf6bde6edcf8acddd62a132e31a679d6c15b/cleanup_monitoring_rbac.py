"""Remove only the unused monitoring permissions created by this campaign."""
import json
import subprocess
import time
from pathlib import Path

A = Path(__file__).resolve().parent
NS = 'kafkastreamingdata'
K = ['kubectl', '--context', 'nautilus', '-n', NS]
record_path = A / 'monitoring-rbac-cleanup.json'
assert not record_path.exists(), 'Inspect the existing cleanup record before retrying'
assert json.loads((A / 'campaign-restoration-status.json').read_text())['status'] == 'restored'
original = json.loads((A / 'restoration-rbac-inspection.json').read_text())['items']
log = (A / 'prepare-cluster.log').read_text()
for message in ('serviceaccount/pipeline-prometheus created', 'role.rbac.authorization.k8s.io/pipeline-prometheus-discovery created', 'rolebinding.rbac.authorization.k8s.io/pipeline-prometheus-discovery created'):
    assert message in log, 'Campaign creation evidence is missing'


def get(*args):
    return json.loads(subprocess.check_output(K + ['get', *args, '-o', 'json'], text=True, timeout=40))


def references_account(obj):
    if isinstance(obj, dict):
        if any(obj.get(k) == 'pipeline-prometheus' for k in ('serviceAccount', 'serviceAccountName')):
            return True
        return any(references_account(v) for v in obj.values())
    if isinstance(obj, list):
        return any(references_account(v) for v in obj)
    return False


workloads = get('deployment,statefulset,daemonset,job,cronjob,pod')
assert not references_account(workloads), 'A workload now uses the account; preserve it'
for binding in get('rolebinding')['items']:
    if binding['metadata']['name'] != 'pipeline-prometheus-discovery':
        assert not any(s.get('name') == 'pipeline-prometheus' and s.get('kind') == 'ServiceAccount' for s in binding.get('subjects', [])), 'Another binding now uses the account'
        assert binding.get('roleRef', {}).get('name') != 'pipeline-prometheus-discovery', 'Another binding now uses the role'

by_kind = {obj['kind']: obj for obj in original}
record = dict(started_epoch=time.time(), status='removing_owned_unused_objects', objects=[])
paths = {
    'RoleBinding': f'/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings/pipeline-prometheus-discovery',
    'Role': f'/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/roles/pipeline-prometheus-discovery',
    'ServiceAccount': f'/api/v1/namespaces/{NS}/serviceaccounts/pipeline-prometheus',
}
for kind in paths:
    obj = by_kind[kind]
    live = get(kind.lower(), obj['metadata']['name'])
    assert live == obj, f'{kind} changed after the ownership inspection; preserve it'
record_path.write_text(json.dumps(record, indent=2) + '\n')
for kind, uri in paths.items():
    obj = by_kind[kind]
    # Server-side UID and resource-version preconditions also protect changes
    # between this inspection and deletion. No force deletion is used.
    options = dict(apiVersion='v1', kind='DeleteOptions', preconditions={k: obj['metadata'][k] for k in ('uid', 'resourceVersion')})
    body = A / f'cleanup-{kind}-delete-options.json'
    body.write_text(json.dumps(options) + '\n')
    response = subprocess.check_output(K + ['delete', '--raw', uri, '-f', str(body)], text=True, timeout=40)
    record['objects'].append(dict(kind=kind, name=obj['metadata']['name'], uid=obj['metadata']['uid'], response=json.loads(response)))
    record_path.write_text(json.dumps(record, indent=2) + '\n')
for kind, obj in by_kind.items():
    result = subprocess.check_output(K + ['get', kind.lower(), obj['metadata']['name'], '--ignore-not-found', '-o', 'name'], text=True, timeout=40)
    assert not result.strip(), 'Deletion not yet verified'
record.update(status='removed_and_verified', finished_epoch=time.time(), inspected_workload_objects=len(workloads['items']))
record_path.write_text(json.dumps(record, indent=2) + '\n')
print(record_path)
