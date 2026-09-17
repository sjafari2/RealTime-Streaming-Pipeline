"""Check the actual saved block configuration before any cluster access."""
import importlib.util
import json
from pathlib import Path
import pytest
import yaml

spec = importlib.util.spec_from_file_location('controlled_block_workload', Path(__file__).resolve().parents[1] / 'experiments/controlled-followup-20260912/execute_block.py')
block = importlib.util.module_from_spec(spec)
spec.loader.exec_module(block)

@pytest.mark.parametrize('order,actions', [('scale-first',['scale','none']), ('keep-first',['none','scale'])])
@pytest.mark.parametrize('workload,partition', [('single-partition','0'),('80-20','0.2')])
def test_workload_is_frozen_before_cluster_access(tmp_path, monkeypatch, workload, partition, order, actions):
    class BeforeCluster(Exception): pass
    def stop_before_cluster(): raise BeforeCluster()
    monkeypatch.setattr(block.r, 'command_lock', stop_before_cluster)
    audit = tmp_path / 'block'
    with pytest.raises(BeforeCluster):
        block.execute(audit, static_startup=True, include_comparison=True, workload=workload, trial_order=order)
    cfg = yaml.safe_load((audit/'experiment-config.yaml').read_text())['data']
    assert cfg['SKEW_PARTITION'] == partition
    assert cfg['SKEW_FRACTION'] == '0.8'
    assert int(cfg['NUM_PARTITIONS']) == 60
    assert int(cfg['TARGET_RATE']) * int(cfg['PRODUCER_POD_COUNT']) == 1500
    assert cfg['CONSUMER_STATIC_MEMBERSHIP'] == 'true'
    assert json.loads((audit/'block-status.json').read_text())['workload'] == workload
    assert json.loads((audit/'block-status.json').read_text())['order'] == actions

@pytest.mark.parametrize('workload', ['80-20','unknown'])
def test_invalid_protocol_stops_before_creating_audit(tmp_path, workload):
    audit = tmp_path/'block'
    with pytest.raises(ValueError): block.execute(audit, workload=workload)
    assert not audit.exists()


@pytest.mark.parametrize('workload,rate,duration', [
    ('balanced-low',200,180), ('balanced-short',500,180), ('balanced-sustained',500,600), ('stability-pressure',500,1260)])
def test_balanced_matched_config_is_frozen_before_cluster_access(tmp_path, monkeypatch, workload, rate, duration):
    class BeforeCluster(Exception): pass
    def stop_before_cluster(): raise BeforeCluster()
    monkeypatch.setattr(block.r, 'command_lock', stop_before_cluster)
    audit = tmp_path / 'balanced'
    with pytest.raises(BeforeCluster):
        block.execute(audit, static_startup=True, include_comparison=True, workload=workload)
    config=yaml.safe_load((audit/'experiment-config.yaml').read_text())['data']
    assert config['TRAFFIC_MODE']=='balanced'
    assert int(config['TARGET_RATE'])==rate
    assert int(config['EXP_DURATION_SEC'])==duration
    assert int(config['WARMUP_SECONDS'])==60
    assert int(config['DRAIN_SECONDS'])==120
    assert config['CONSUMER_STATIC_MEMBERSHIP']=='true'
    assert config['CONSUMER_ASSIGNMENT_MODE']=='cooperative'
    assert int(config['WORKLOAD_SEED'])==71


def test_stability_scaling_time_is_frozen_before_cluster_access(tmp_path, monkeypatch):
    class BeforeCluster(Exception): pass
    def stop(): raise BeforeCluster()
    monkeypatch.setattr(block.r, 'command_lock', stop)
    audit = tmp_path/'stability'
    with pytest.raises(BeforeCluster):
        block.execute(audit, static_startup=True, include_comparison=True, workload='stability-pressure', trial_order='keep-first')
    saved=json.loads((audit/'block-status.json').read_text())
    assert saved['intervention_after_seconds']==300
    assert saved['order']==['none','scale']
