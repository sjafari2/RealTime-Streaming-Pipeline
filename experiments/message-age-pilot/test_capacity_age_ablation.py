import unittest
from capacity_age_ablation import forecast, assigned_rates


class ForecastChecks(unittest.TestCase):
    def test_age_changes_deadline_but_not_service_requirement(self):
        records = [[str(i), 0, i, 0] for i in range(100)]
        blind = forecast(records, [0], [10], 15, False)
        aware = forecast(records, [0], [10], 15, True)
        self.assertAlmostEqual(aware['p99_seconds']-blind['p99_seconds'], 15)
        self.assertTrue(blind['meets_deadline_objective'])
        self.assertFalse(aware['meets_deadline_objective'])

    def test_individual_rates_preserve_partition_ownership(self):
        records = [[str(i), i % 2, i, 0] for i in range(100)]
        self.assertAlmostEqual(forecast(records, [0, 1], [10, 100], 0, False)['p99_seconds'], 4.9)
        self.assertEqual(assigned_rates([300, 100], [0, 1], 3), [300, 100, 0])

    def test_nearest_rank_p99_matches_allowed_deadline_misses(self):
        records = [[str(i), 0, i, 0] for i in range(100)]
        result = forecast(records, [0], [1], 0, False, deadline=99)
        self.assertEqual(result['deadline_misses'], 1)
        self.assertTrue(result['meets_deadline_objective'])

    def test_invalid_future_creation_is_rejected(self):
        with self.assertRaises(ValueError):
            forecast([['a', 0, 0, 2]], [0], [100], 1, True)


if __name__ == '__main__':
    unittest.main()
