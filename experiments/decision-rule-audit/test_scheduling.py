"""Check ordering statistics and missing/ambiguous monitoring observations."""
import unittest
from analyze_scheduling import describe, monitoring


def record(partition, offset, created, start, producer='a', epoch=1):
    return dict(topic='t', partition=partition, offset=offset,
                message_id='run/' + producer + '/' + str(offset) + '-' + str(partition),
                assignment_epoch=epoch, producer_timestamp=created,
                processing_start_timestamp=start, completion_timestamp=start + .001,
                processing_seconds=.001)


def sample(name, value, pod='c0', timestamp=30):
    return dict(metric={'__name__': name, 'topic': 't', 'partition': '0',
                        'pod': pod, 'incarnation': 'i'}, values=[[timestamp, str(value)]])


class SchedulingTests(unittest.TestCase):
    def test_streaks_are_not_batches(self):
        rows = [record(p, i, i, i + 1) for i, p in enumerate([0, 0, 0, 1, 0])]
        result = describe(rows)
        self.assertEqual(result['streak_histogram'], {1: 2, 3: 1})
        self.assertEqual(result['streak_max'], 3)
        self.assertEqual(result['same_partition_adjacent_fraction'], .5)

    def test_same_producer_clock_only(self):
        rows = [record(0, 0, 100, 200, 'a'), record(1, 0, 1, 201, 'b'),
                record(2, 0, 98, 202, 'a')]
        result = describe(rows)
        self.assertEqual(result['same_producer_cross_partition_pairs'], 1)
        self.assertEqual(result['largest_creation_time_backtrack_seconds'], 2)
        self.assertEqual(result['creation_time_backtracks_over_1s'], 1)

    def test_replayed_offsets_after_assignment_are_separate(self):
        rows = [record(0, 9, 0, 1), record(0, 8, 0, 2, epoch=2),
                record(0, 8, 0, 3, epoch=2)]
        result = describe(rows)
        self.assertEqual(result['nonincreasing_offsets_within_epoch'], 1)
        self.assertEqual(result['selected_processing_starts'], 3)

    def test_overlap_is_reported(self):
        result = describe([record(0, 0, 0, 1), record(1, 0, 0, 1)])
        self.assertEqual(result['overlapping_or_reversed_processing_times'], 1)

    def test_empty_stream(self):
        result = describe([])
        self.assertEqual(result['observed_start_rate'], 0)
        self.assertIsNone(result['same_partition_adjacent_fraction'])

    def valid_samples(self):
        return [sample('consumer_partition_owned', 1), sample('consumer_processing_backlog', 10),
                sample('consumer_lag_valid', 1), sample('consumer_lag_observation_age_seconds', 1)]

    def test_valid_backlog(self):
        result = monitoring(self.valid_samples(), 0)
        self.assertEqual(result[('c0', 'i')][0], [10])

    def test_missing_stale_invalid_or_duplicate_owner_is_not_zero(self):
        series = self.valid_samples()
        cases = [series[:3], series[:3] + [sample('consumer_lag_observation_age_seconds', 11)],
                 series + [sample('consumer_partition_owned', 1, 'c1')],
                 [sample('consumer_partition_owned', 1), sample('consumer_processing_backlog', 'nan'),
                  sample('consumer_lag_valid', 1), sample('consumer_lag_observation_age_seconds', 1)]]
        for case in cases:
            self.assertFalse(dict(monitoring(case, 0)))

    def test_window_is_half_open_with_millisecond_query_precision(self):
        series = self.valid_samples()
        for s in series:
            s['values'] += [[60, s['values'][0][1]]]
        self.assertEqual(monitoring(series, .0003)[('c0', 'i')][0], [10])


if __name__ == '__main__':
    unittest.main()
