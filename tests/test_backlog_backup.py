import importlib.util
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
