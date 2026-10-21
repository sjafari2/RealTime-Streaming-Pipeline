"""Replacement discovery must not adopt the retired producer's identity or storage."""
import copy
import json
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as runner
import node_placement
from placement_control import APP_NAMES, POD_SELECTOR
from test_node_placement import deployment


def test_runtime_selects_replacement_producers(monkeypatch):
    def query(*args, **kwargs):
        assert args == ('get', 'pods', '-l', 'app=producer-workers', '-o', 'json')
        return json.dumps({'items': [
            {'metadata': {'name': 'producer-workers-0'}},
            {'metadata': {'name': 'producer-workers-1', 'deletionTimestamp': 'now'}}]}).encode()
    monkeypatch.setattr(runner, 'kubectl', query)
    assert runner.pods('producer') == ['producer-workers-0']
    assert 'producer-sts' not in POD_SELECTOR


def test_retired_pod_is_outside_new_run_membership():
    items, sets, expected, config = deployment()
    retired = copy.deepcopy(items[0])
    retired['metadata'].update(name='producer-sts-1', deletionTimestamp='earlier')
    retired['metadata']['labels']['app'] = 'producer-sts'
    retired['spec']['nodeName'] = 'retired-unreachable-node'
    items.append(retired)
    report = node_placement.observe(items, expected, runner.NS, node_placement.policy(config))
    assert report['valid'], report['issues']
    assert 'producer-sts-1' not in [p['pod'] for p in report['pods']]


def test_replacement_manifests_share_new_config_and_preserve_workload_ordinals():
    producer = yaml.safe_load((ROOT / 'k8s/statefullsets/producer-sts.yaml').read_text())
    consumer = yaml.safe_load((ROOT / 'k8s/statefullsets/consumer-sts.yaml').read_text())
    assert producer['metadata']['name'] == APP_NAMES['producer']
    assert producer['spec']['selector']['matchLabels'] == {'app': APP_NAMES['producer']}
    for item in (producer, consumer):
        volumes = {v['name']: v for v in item['spec']['template']['spec']['volumes']}
        assert volumes['pipeline-config']['persistentVolumeClaim']['claimName'] == 'pipeline-config-20261020'
    volumes = {v['name']: v for v in producer['spec']['template']['spec']['volumes']}
    assert volumes['producer-data']['persistentVolumeClaim']['claimName'] == 'producer-data-20261020'
    env = {v['name']: v for v in producer['spec']['template']['spec']['containers'][0]['env']}
    assert env['PRODUCER_WORKLOAD_ID']['valueFrom']['fieldRef']['fieldPath'] == "metadata.labels['apps.kubernetes.io/pod-index']"
    service = yaml.safe_load((ROOT / 'k8s/services/producer-svc.yaml').read_text())
    assert service['spec']['selector'] == {'app': APP_NAMES['producer']}
