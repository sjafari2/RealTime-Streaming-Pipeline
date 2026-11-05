import importlib.util
import os
from pathlib import Path

import numpy as np

root = Path(os.environ['PIPELINE_REPOSITORY'])
source = Path(os.environ.get('REPEAT_MEANS_SCRIPT', root / 'experiments/twelve-hot600-20261104/plot_repeat_means.py'))
spec = importlib.util.spec_from_file_location('repeat_means', source)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_means_require_both_runs_and_keep_negative_growth():
    pair = [{'growth': [[0, -4], [2, None], [4, 6]]},
            {'growth': [[.0004, -2], [2, 8]]}]
    grid, mean, low, high = m.pair_mean(pair, 'growth', 2)
    assert mean[0] == -3 and low[0] == -4 and high[0] == -2
    assert np.isnan(mean[1]) and np.isnan(mean[2])


def test_invalid_duplicate_cannot_be_overwritten_by_valid_sample():
    _, values = m.align([[2, None], [2, 10], [6.8, 9]], 2)
    assert np.isnan(values[1]) and np.isnan(values[3])


def test_resource_sum_requires_all_started_fresh_consumers():
    data = []
    for pod, values, stamps in [('c0', [(0, 100), (2, 100), (4, 100)], [(0, 0), (2, 2), (4, 4)]),
                                ('c1', [(4, 50)], [(4, -20)])]:
        data += [dict(metric={'__name__': 'cpu', 'pod': pod}, values=values),
                 dict(metric={'__name__': 'cpu_scrape_timestamp_seconds', 'pod': pod}, values=stamps)]
    assert m.total_resource(data, 'cpu', 100, {'c0': 0, 'c1': 1}, 0, 6) == [[0, 1], [2, None], [4, None]]
