"""Check that a truncated observation does not create a false throughput decline."""
import json
from pathlib import Path
import sys

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python-scripts'))
from comparison_series import throughput_bins


@pytest.mark.parametrize('duration', [6, 16, 20])
def test_constant_rate_with_partial_final_bin(tmp_path, duration):
    rows = []
    for i in range(duration * 10):
        timestamp = 100 + (i + .5) / 10
        rows.extend([
            dict(event='acknowledged', message_id=str(i), producer_timestamp=timestamp),
            dict(event='completed', message_id=str(i), completion_timestamp=timestamp),
        ])
    # Duplicate evidence must not double-count a record. Boundary events are outside the interval.
    rows.extend(rows[:2])
    rows.extend([
        dict(event='acknowledged', message_id='after', producer_timestamp=100 + duration),
        dict(event='completed', message_id='after', completion_timestamp=100 + duration),
    ])
    (tmp_path / 'events.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
    times, rates = throughput_bins(tmp_path, 100, 100 + duration)
    for rate in rates:
        np.testing.assert_allclose(rate, 10)
    assert 0 < times[-1] < duration / 60


@pytest.mark.parametrize('start,end,width', [(1, 1, 10), (2, 1, 10), (0, 2, 0), (0, float('inf'), 10)])
def test_invalid_throughput_intervals(tmp_path, start, end, width):
    with pytest.raises(ValueError):
        throughput_bins(tmp_path, start, end, width)
