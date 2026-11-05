import importlib.util
import os
from pathlib import Path

import pytest

root = Path(os.environ['PIPELINE_REPOSITORY'])
source = Path(os.environ.get('PARTITION_MEANS_SCRIPT', root / 'experiments/twelve-hot600-20261104/compare_partition_means.py'))
spec = importlib.util.spec_from_file_location('partition_means', source)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_selects_original_nonoverlapping_windows():
    rows = [dict(end_after_evaluation_seconds=t, duration_seconds=30, marker=t) for t in range(30, 601, 2)]
    assert [r['marker'] for r in m.select_intervals(rows)] == list(range(30, 601, 30))
    with pytest.raises(ValueError):
        m.select_intervals(rows + [rows[0]])


def test_resource_time_mean_does_not_bridge_missing_samples():
    result = m.time_mean([[0, 1], [2, 3], [4, None], [6, 100], [12, 100]])
    assert result['mean'] == 2 and result['covered_seconds'] == 2


def test_cached_series_cannot_hide_missing_observation():
    with pytest.raises(ValueError):
        m.check_series([[0, None]], [[0, 0]])
