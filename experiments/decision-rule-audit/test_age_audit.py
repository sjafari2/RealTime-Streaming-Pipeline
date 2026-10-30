"""Age accounting and baseline checks with hand-calculated expected outcomes."""
import unittest
from age_audit import candidate_choice,fixture
from transition_audit import decide,simulate,spread_assignment


class AgeAuditChecks(unittest.TestCase):
    def test_rule_and_feasibility(self):
        rates,backlog,current=fixture()
        d=decide(rates,backlog,current,3,300,10,1)
        self.assertEqual((d['action'],d['consumers']),('REASS',3))
        self.assertTrue(d['projected_constraints_satisfied'])

    def test_aged_deadline_crossing_matches_hand_calculation(self):
        rates,backlog,current=fixture()
        decision=decide(rates,backlog,current,3,300,10,1)
        outcomes=[]
        for age in (0,6):
            value=simulate(rates,backlog,decision['owners'],[300]*3,
                           pause=1,cutoff=20,deadline=10,initial_age=age)
            self.assertEqual(value['unfinished_by_cutoff'],0)
            self.assertAlmostEqual(value['p99_completed_by_cutoff_seconds'],4.96+age)
            outcomes.append(value)
        self.assertEqual(outcomes[0]['deadline_misses_known_by_cutoff'],0)
        self.assertEqual(outcomes[1]['deadline_misses_known_by_cutoff'],900)

    def test_added_capacity_does_not_always_rescue_the_objective(self):
        rates,backlog,_=fixture()
        for age,passes in ((6,True),(9.5,False)):
            value=simulate(rates,backlog,spread_assignment(6),[300]*6,
                           pause=1,cutoff=20,deadline=10,initial_age=age)
            self.assertEqual(value['p99_completed_by_cutoff_seconds']<=10,passes)

    def test_infeasible_candidates_are_not_forced_into_a_choice(self):
        self.assertIsNone(candidate_choice([{'response':'a','unfinished_by_cutoff':1,
            'p99_completed_by_cutoff_seconds':1,'requested_consumer_seconds_to_cutoff':3}],10))


if __name__=='__main__': unittest.main()
