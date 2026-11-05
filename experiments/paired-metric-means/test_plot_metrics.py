"""Checks for censoring, age populations and equal-weight run comparisons."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('plot_metrics', Path(__file__).with_name('plot_metrics.py'))
metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metrics)


class MetricChecks(unittest.TestCase):
    def test_age_matches_direct_population(self):
        # Include a completion received before its producer acknowledgment.
        ack = {'a': (99., 100.), 'b': (101., 105.), 'c': (103., 103.)}
        done = {'a': (104.,), 'b': (104.,)}
        observed = dict(metrics.age_series(ack, done, 100., 110.))
        for seconds, mean in observed.items():
            now = 100. + seconds
            pending = [now-created for key,(created,admitted) in ack.items()
                if admitted <= now and (key not in done or done[key][0] > now)]
            expected = sum(pending)/len(pending) if pending else None
            self.assertEqual(mean, expected)

    def test_unknown_recovery_prevents_mean(self):
        self.assertIsNone(metrics.average([112., None]))
        self.assertIsNone(metrics.average([None, None]))
        self.assertEqual(metrics.average([0., 0.]), 0.)

    def test_percentiles_receive_run_weight(self):
        self.assertEqual(metrics.average([10., 50.]), 30.)
        self.assertIsNone(metrics.average([10.]))

    def test_missing_repetition_rejected(self):
        with self.assertRaises(AssertionError):
            metrics.pair([{'condition':'smaller','arm':'keep3','run':1}], 'smaller','keep3')

    def test_missing_resource_observations_are_not_zero(self):
        self.assertEqual(metrics.resource_cost({'process_resources':[]}, 'consumer_cpu_percent',100), (None,None))


if __name__ == '__main__':
    unittest.main()
