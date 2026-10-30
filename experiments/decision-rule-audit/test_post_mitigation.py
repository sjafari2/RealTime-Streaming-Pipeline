"""Boundary and censoring checks for the saved waiting-cohort analysis."""
import unittest

from analyze_post_mitigation import Ack, Done, cohort_stats, decision_groups, ownership_coverage, pending_ids


def completed(key, created, finished, partition=0):
    return Done(key,finished,created,'consumer-0','process',1,'topic',partition,0,
                finished-.01,.01,'hash')


class CohortTests(unittest.TestCase):
    def stats(self, ids, ack, done, anchor=10, activation=20, end=30, cutoff=40, deadline=1):
        return cohort_stats(ids,ack,done,anchor,activation,end,cutoff,deadline)

    def test_four_decision_groups_are_disjoint_and_exhaustive(self):
        ack = {'waiting':Ack(5,6,'topic',0,0), 'finished':Ack(5,6,'topic',0,1),
               'late_ack':Ack(9,11,'topic',0,2), 'later':Ack(10,10,'topic',0,3)}
        done = {'finished':completed('finished',5,10)}
        groups = decision_groups(ack,done,10)
        self.assertEqual(groups,{'pending_at_decision':{'waiting'},
            'completed_before_decision':{'finished'},'earlier_produced_acknowledged_later':{'late_ack'},
            'produced_after_decision':{'later'}})
        self.assertEqual(sum(map(len,groups.values())),len(ack))

    def test_completion_at_anchor_is_not_pending(self):
        ack = {'a':Ack(5,10,'topic',0,0)}
        self.assertEqual(pending_ids(ack,{},10),{'a'})
        self.assertEqual(pending_ids(ack,{'a':completed('a',5,10)},10),set())

    def test_clearance_fraction_uses_all_messages_not_only_completions(self):
        ack = {str(i):Ack(5,6,'topic',0,i) for i in range(4)}
        done = {'0':completed('0',5,11),'1':completed('1',5,12)}
        stats = self.stats(set(ack),ack,done)
        self.assertEqual(stats['cohort_50_percent_completed_seconds'],2)
        self.assertIsNone(stats['cohort_95_percent_completed_seconds'])
        self.assertIsNone(stats['all_completed_seconds'])
        self.assertEqual(stats['unfinished_fraction'],.5)
        self.assertEqual(stats['completion_p99_seconds'],7)

    def test_older_cohort_can_clear_before_activation_with_new_work_pending(self):
        ack = {'old':Ack(5,6,'topic',0,0),'new':Ack(16,17,'topic',0,1)}
        done = {'old':completed('old',5,18),'new':completed('new',16,25)}
        old = decision_groups(ack,done,10)['pending_at_decision']
        self.assertEqual(self.stats(old,ack,done)['all_completed_seconds'],8)
        at_activation = pending_ids(ack,done,20)
        self.assertEqual(at_activation,{'new'})
        self.assertEqual(self.stats(at_activation,ack,done,anchor=20)['all_completed_seconds'],5)

    def test_unfinished_deadline_known_or_censored(self):
        ack = {'expired':Ack(5,6,'topic',0,0),'recent':Ack(39.5,39.6,'topic',0,1)}
        stats = self.stats(set(ack),ack,{})
        self.assertEqual(stats['deadline_misses_known_by_cutoff'],1)
        self.assertEqual(stats['deadline_censored'],1)
        self.assertEqual(stats['unfinished'],2)

    def test_post_cutoff_completion_is_unfinished(self):
        ack = {'a':Ack(5,6,'topic',0,0)}
        stats = self.stats({'a'},ack,{'a':completed('a',5,41)})
        self.assertEqual(stats['completed_by_cutoff'],0)
        self.assertIsNone(stats['completion_p99_seconds'])
        self.assertIsNone(stats['all_completed_seconds'])

    def test_cutoff_completion_is_included(self):
        ack = {'a':Ack(5,6,'topic',0,0)}
        stats = self.stats({'a'},ack,{'a':completed('a',5,40)})
        self.assertEqual(stats['completed_by_cutoff'],1)
        self.assertEqual(stats['all_completed_seconds'],30)

    def test_empty_cohort_has_no_clearance_or_latency(self):
        stats = self.stats(set(),{}, {})
        self.assertIsNone(stats['all_completed_seconds'])
        self.assertIsNone(stats['completion_p99_seconds'])
        self.assertEqual(stats['clearance_curve'],[])

    def test_invalid_and_changed_ownership_remain_visible(self):
        snapshots = [dict(timestamp=t,valid=v,owners={'topic/0':owner+'/inc'})
                     for t,v,owner in [(10,True,'c0'),(12,False,'c0'),(14,True,'c1'),(16,True,'c0')]]
        self.assertEqual(ownership_coverage(snapshots,{0:'c0'},10,16),
                         dict(expected_samples=3,recorded_samples=3,valid_samples=2,
                              matching_activation_owner_samples=1))


if __name__ == '__main__':
    unittest.main()
