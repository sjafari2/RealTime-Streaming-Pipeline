import gzip
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python-scripts'))
from compress_monitoring_csv import compress_csv


def test_verified_csv_can_be_recovered_and_repeated_safely(tmp_path):
    (tmp_path/'manifest.json').write_text(json.dumps({'run_id':'r'}))
    (tmp_path/'runner-status.json').write_text(json.dumps({'run_id':'r','status':'complete'}))
    data = b'timestamp,value\n1,NaN\n2,4\n' * 500
    (tmp_path/'prometheus.csv').write_bytes(data)
    record = compress_csv(tmp_path)
    assert gzip.decompress((tmp_path/'prometheus.csv.gz').read_bytes()) == data
    assert not (tmp_path/'prometheus.csv').exists()
    assert compress_csv(tmp_path) == record


def test_incomplete_run_is_never_modified(tmp_path):
    (tmp_path/'manifest.json').write_text(json.dumps({'run_id':'r'}))
    (tmp_path/'runner-status.json').write_text(json.dumps({'run_id':'r','status':'failed'}))
    (tmp_path/'prometheus.csv').write_bytes(b'original')
    with pytest.raises(ValueError):
        compress_csv(tmp_path)
    assert (tmp_path/'prometheus.csv').read_bytes() == b'original'
