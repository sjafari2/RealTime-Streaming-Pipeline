"""Checks against small queues whose outcomes can be computed by hand."""
import unittest

from transition_audit import decide, fixture, predict_initial, simulate


class QueueChecks(unittest.TestCase):
    def test_known_fifo_queue_and_deadline(self):
        result = simulate([5], [100], [0], [10], cutoff=10, deadline=8)
        self.assertEqual(result["completed_by_cutoff"], 100)
        self.assertEqual(result["deadline_misses_known_by_cutoff"], 20)
        self.assertAlmostEqual(result["p99_completed_by_cutoff_seconds"], 9.9)
        # The last arrival occurs at second 30 and finishes at 30.1.
        self.assertEqual(result["all_completions_by_production_end"], 249)

    def test_unfinished_is_not_automatically_a_deadline_miss(self):
        result = simulate([5], [100], [0], [10], cutoff=5, deadline=8)
        self.assertEqual(result["unfinished_by_cutoff"], 50)
        self.assertEqual(result["deadline_censored_at_cutoff"], 50)
        self.assertEqual(result["deadline_misses_known_by_cutoff"], 0)
        self.assertAlmostEqual(result["p99_completed_by_cutoff_seconds"], 5)

    def test_blocked_partition_does_not_block_ready_partition(self):
        result = simulate([0, 0], [20, 20], [0, 0], [10], cutoff=3, deadline=5,
                          partition_ready=[10, 0])
        self.assertEqual(result["completed_by_cutoff"], 20)
        self.assertEqual(result["unfinished_by_cutoff"], 20)
        self.assertAlmostEqual(result["initial_cohort_last_completion_seconds"], 12)

    def test_age_changes_latency_not_completion_count(self):
        young = simulate([0], [10], [0], [10], cutoff=2, deadline=5)
        aged = simulate([0], [10], [0], [10], cutoff=2, deadline=5, initial_age=5)
        self.assertEqual(young["completed_by_cutoff"], aged["completed_by_cutoff"])
        self.assertEqual(young["deadline_misses_known_by_cutoff"], 0)
        self.assertEqual(aged["deadline_misses_known_by_cutoff"], 10)
        self.assertAlmostEqual(aged["p99_completed_by_cutoff_seconds"] - young["p99_completed_by_cutoff_seconds"], 5)

    def test_reassignment_does_not_remove_idle_replicas(self):
        rates, _, _ = fixture()
        result = decide(rates, [0]*60, [0]*60, 3, 500, 10, 0)
        self.assertEqual(result["action"], "REASS")
        self.assertEqual(result["packed_bins"], 2)
        self.assertEqual(result["consumers"], 3)

    def test_oversized_partition_does_not_receive_false_feasibility(self):
        result = decide([1000, 10], [100, 0], [0, 1], 2, 300, 10, 5)
        self.assertFalse(result["projected_constraints_satisfied"])

    def test_analytic_and_event_results_with_continuing_arrivals(self):
        rates, backlog, owners = fixture()
        self.assertEqual(sum(rates[:12]), .8 * sum(rates))
        for pause in (0, 1, 5):
            decision = decide(rates, backlog, owners, 3, 300, 10, pause)
            caps = [300]*decision["consumers"]
            predicted = predict_initial(backlog, decision["owners"], caps, pause)
            simulated = simulate(rates, backlog, decision["owners"], caps, pause)
            self.assertEqual(predicted["unfinished_by_cutoff"], simulated["unfinished_by_cutoff"])
            self.assertAlmostEqual(predicted["p99_followed_to_completion_seconds"],
                                   simulated["initial_cohort_p99_followed_to_completion_seconds"])

    def test_no_work_returns_no_percentile(self):
        result = simulate([0], [0], [0], [300])
        self.assertIsNone(result["p99_completed_by_cutoff_seconds"])
        self.assertEqual(result["unfinished_by_cutoff"], 0)

    def test_short_pause_helps_but_long_pause_hurts_initial_fifo_cohort(self):
        rates, backlog, current = fixture()
        keep = simulate(rates, backlog, current, [300]*3)
        for pause, improves in ((1, True), (5, False)):
            decision = decide(rates, backlog, current, 3, 300, 10, pause)
            self.assertTrue(decision["projected_constraints_satisfied"])
            result = simulate(rates, backlog, decision["owners"], [300]*decision["consumers"], pause)
            self.assertEqual(result["initial_cohort_p99_followed_to_completion_seconds"] < keep["initial_cohort_p99_followed_to_completion_seconds"], improves)
        self.assertEqual(decision["action"], "UP")
        self.assertEqual(decision["consumers"], 4)

    def test_scheduling_can_reverse_the_candidate_guards_recommendation(self):
        rates, backlog, current = fixture()
        decision = decide(rates, backlog, current, 3, 300, 10, 5)
        keep = simulate(rates, backlog, current, [300]*3, discipline="round_robin")
        act = simulate(rates, backlog, decision["owners"], [300]*4, 5, discipline="round_robin")
        self.assertLess(act["unfinished_by_cutoff"], keep["unfinished_by_cutoff"])
        self.assertLess(act["deadline_misses_known_by_cutoff"], keep["deadline_misses_known_by_cutoff"])


if __name__ == "__main__":
    unittest.main()
