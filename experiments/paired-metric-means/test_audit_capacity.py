"""Capacity pooling preserves exposure time and unavailable estimates."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('audit_capacity', Path(__file__).with_name('audit_capacity.py'))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class CapacityChecks(unittest.TestCase):
    def test_pool_uses_observed_time_and_excludes_ineligible_windows(self):
        records = [dict(run_id='one', windows=[
            dict(eligible=True, observed_seconds=60, completions=6000, effective_rate=100),
            dict(eligible=False, observed_seconds=60, completions=60000, effective_rate=1000),
        ]), dict(run_id='two', windows=[
            dict(eligible=True, observed_seconds=30, completions=6000, effective_rate=200),
        ])]
        result = audit.aggregate(records)
        self.assertAlmostEqual(result['effective_rate'], 12000/90)
        self.assertEqual(result['eligible_windows'], 2)
        self.assertEqual(result['contributing_runs'], ['one', 'two'])
        self.assertEqual(result['minimum_window_rate'], 100)
        self.assertEqual(result['maximum_window_rate'], 200)

    def test_missing_capacity_is_not_zero(self):
        result = audit.aggregate([dict(run_id='idle', windows=[dict(eligible=False)])])
        self.assertIsNone(result['effective_rate'])
        self.assertIsNone(result['minimum_window_rate'])
        self.assertEqual(result['eligible_windows'], 0)


if __name__ == '__main__':
    unittest.main()
