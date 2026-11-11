import unittest
from repetition_statistics import complete_summary, validate_design


class RepetitionStatisticsTests(unittest.TestCase):
    def test_standard_deviation_uses_independent_runs(self):
        result = complete_summary([10, 20, 30])
        self.assertEqual(result['mean'], 20)
        self.assertEqual(result['sample_sd'], 10)
        self.assertEqual(result['individual'], [10, 20, 30])

    def test_unconfirmed_recovery_prevents_an_unconditional_mean(self):
        result = complete_summary([10, None, 30])
        self.assertIsNone(result['mean'])
        self.assertIsNone(result['sample_sd'])
        self.assertEqual(result['available_runs'], 2)
        self.assertEqual(result['total_runs'], 3)

    def test_one_repetition_has_no_estimated_standard_deviation(self):
        result = complete_summary([7])
        self.assertEqual(result['mean'], 7)
        self.assertIsNone(result['sample_sd'])

    def test_duplicate_runs_cannot_inflate_repetitions(self):
        rows = [dict(backlog_condition='hot12', arm='keep3', run_number=n, run_id=f'run-{n}') for n in [1, 2, 3]]
        validate_design(rows, ['hot12'], ['keep3'], [1, 2, 3])
        with self.assertRaises(ValueError):
            validate_design(rows + [rows[0]], ['hot12'], ['keep3'], [1, 2, 3])
        with self.assertRaises(ValueError):
            validate_design(rows[:-1], ['hot12'], ['keep3'], [1, 2, 3])

    def test_undefined_values_remain_missing(self):
        self.assertIsNone(complete_summary([1, float('nan')])['mean'])
        self.assertIsNone(complete_summary([])['mean'])


if __name__ == '__main__':
    unittest.main()
