import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('backlog_backup', ROOT / 'experiments/backlog-comparison/run.py')
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def test_verified_backup_can_be_rechecked_without_overwriting(tmp_path):
    source = tmp_path / 'source/run-test'
    source.mkdir(parents=True)
    (source / 'evidence.json').write_text('{"messages": 3}\n')
    destination = Path(campaign.backup_directory(source, tmp_path / 'backup'))
    assert (destination / 'evidence.json').read_bytes() == (source / 'evidence.json').read_bytes()
    assert (destination / 'backup-sha256.json').exists()
    assert campaign.backup_directory(source, tmp_path / 'backup') == str(destination)


def test_existing_conflicting_backup_is_preserved_and_rejected(tmp_path):
    source = tmp_path / 'source/run-test'
    source.mkdir(parents=True)
    (source / 'evidence.json').write_text('source')
    target = tmp_path / 'backup/run-test/evidence.json'
    target.parent.mkdir(parents=True)
    target.write_text('different')
    with pytest.raises(ValueError, match='Backup differs'):
        campaign.backup_directory(source, tmp_path / 'backup')
    assert target.read_text() == 'different'


def test_resource_append_after_cutoff_retains_previous_backup(tmp_path):
    source = tmp_path / 'source/run-test'
    source.mkdir(parents=True)
    (source / 'manifest.json').write_text(json.dumps(dict(drain_end_epoch=100)))
    resource = source / 'resource-history.jsonl'
    first = '{"timestamp": 90}\n'
    resource.write_text(first)
    destination = Path(campaign.backup_directory(source, tmp_path / 'backup'))
    resource.write_text(first+'{"timestamp": 110}\n')
    campaign.backup_directory(source, tmp_path / 'backup')
    assert (destination / resource.name).read_bytes() == resource.read_bytes()
    assert next((destination / 'versions').iterdir()).read_text() == first
    assert json.loads((destination / 'backup-revisions.jsonl').read_text())['additional_records'] == 1


def test_resource_append_inside_measurement_interval_is_rejected(tmp_path):
    source = tmp_path / 'source/run-test'
    source.mkdir(parents=True)
    (source / 'manifest.json').write_text(json.dumps(dict(drain_end_epoch=100)))
    resource = source / 'resource-history.jsonl'
    resource.write_text('{"timestamp": 90}\n')
    campaign.backup_directory(source, tmp_path / 'backup')
    resource.write_text('{"timestamp": 90}\n{"timestamp": 95}\n')
    with pytest.raises(ValueError, match='Backup differs'):
        campaign.backup_directory(source, tmp_path / 'backup')
