"""Check sampled means and prevent missing resource observations becoming zero."""
import importlib.util
import os
from pathlib import Path

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[1]))
SCRIPT = Path(os.environ.get('SIGNAL_WINDOW_SCRIPT', ROOT/'experiments/four-hot600-20261021/analyze_signal_windows.py'))
spec = importlib.util.spec_from_file_location('signal_windows', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_window_excludes_invalid_and_right_boundary_samples():
    rows = [dict(timestamp=0, valid=True, total_lag=10),
            dict(timestamp=2, valid=False, total_lag=10000),
            dict(timestamp=4, valid=True, total_lag=30),
            dict(timestamp=6, valid=True, total_lag=90)]
    result = m.signal_window(rows, 0, 6)
    assert result['fields']['total_lag'] == dict(samples=2, mean=20, minimum=10, maximum=30)
    assert result['valid_observation_fraction'] == 2/3
    assert result['fields']['skew']['mean'] is None


def test_resource_totals_require_every_pod_and_fresh_scrapes():
    series = []
    for pod, value in [('c0', 100), ('c1', 50)]:
        for suffix, values in [('cpu_percent', [[0, value], [2, value], [4, value]]),
                               ('cpu_percent_scrape_timestamp_seconds', [[0, 0], [2, 2], [4, -7]])]:
            series.append(dict(metric={'__name__': 'consumer_'+suffix, 'pod': pod}, values=values))
    result = m.process_window(series, 'consumer', ['c0', 'c1'], 0, 6)
    assert result['cpu_percent']['total'] == dict(samples=2, mean=1.5, minimum=1.5, maximum=1.5)
    assert result['memory_bytes']['total']['mean'] is None
    assert m.process_window(series, 'consumer', ['c0', 'c1', 'missing'], 0, 6)['cpu_percent']['total']['mean'] is None


def test_export_rounding_keeps_nominal_before_grid():
    origin = 1_790_000_000.0004
    rows = [dict(timestamp=round(origin+s, 3), valid=True, total_lag=s, skew=2)
            for s in range(28, 64, 2)]
    result = m.signal_window(rows, origin+30, origin+60, origin)
    assert result['fields']['total_lag']['mean'] == 44
    assert result['expected_observations'] == result['valid_observations'] == 15
    assert result['selected_timestamps'][0] == round(origin+30, 3)
    assert result['selected_timestamps'][-1] == round(origin+58, 3)


def test_missing_grid_points_reduce_coverage_and_activation_is_not_rounded():
    rows = [dict(timestamp=t, valid=True, total_lag=t, skew=1) for t in (60, 64, 66)]
    result = m.signal_window(rows, 60.1, 68, 0)
    assert result['expected_observations'] == 3
    assert result['valid_observations'] == 2
    assert result['valid_grid_fraction'] == 2/3
    assert result['fields']['total_lag']['mean'] == 65


def test_ambiguous_or_off_grid_observations_are_rejected():
    import pytest
    with pytest.raises(ValueError, match='Duplicate'):
        m.signal_window([dict(timestamp=0, valid=False), dict(timestamp=.0004, valid=False)], 0, 2, 0)
    with pytest.raises(ValueError, match='two-second'):
        m.signal_window([dict(timestamp=.01, valid=False)], 0, 2, 0)


def test_process_samples_use_the_same_nominal_grid():
    origin = 1_790_000_000.0004
    values = [[round(origin+s, 3), s*100] for s in range(28, 64, 2)]
    series = [dict(metric={'__name__':'consumer_cpu_percent','pod':'c0'}, values=values),
              dict(metric={'__name__':'consumer_cpu_percent_scrape_timestamp_seconds','pod':'c0'},
                   values=[[t,t] for t,_ in values])]
    result = m.process_window(series, 'consumer', ['c0'], origin+30, origin+60, origin)
    assert result['cpu_percent']['total']['mean'] == 44
    assert result['cpu_percent']['total']['samples'] == 15
