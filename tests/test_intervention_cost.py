"""Boundary and missing-observation checks for intervention cost analysis."""
import importlib.util
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('intervention_cost',ROOT/'python-scripts/intervention_cost.py')
cost=importlib.util.module_from_spec(spec);spec.loader.exec_module(cost)


class InterventionCostTests(unittest.TestCase):
    def test_completion_before_ack_does_not_make_negative_outstanding_work(self):
        ack=[2.,4.,5.];done=[1.,6.,8.]
        self.assertEqual(cost.outstanding(ack,done,5.),2)
        self.assertEqual(cost.outstanding(ack,done,6.),1)

    def snapshots(self):
        return [dict(timestamp=t,valid=True,processing_backlog=50,owners={'0':'c0'},
                     highs={'0':t+100},positions={'0':t+50}) for t in range(0,62,2)]

    def test_recovery_requires_full_hold_not_a_single_low_sample(self):
        rows=self.snapshots();rows[5]['processing_backlog']=101
        result=cost.recovery(rows,0,60,100)
        self.assertEqual(result['first_low_epoch'],12)
        self.assertEqual(result['confirmed_epoch'],42)

    def test_gap_or_owner_change_resets_recovery(self):
        rows=self.snapshots();rows=[r for r in rows if r['timestamp']!=10]
        for row in rows:
            if row['timestamp']>=20:row['owners']={'0':'c1'}
        result=cost.recovery(rows,0,60,100)
        self.assertEqual(result['first_low_epoch'],20)
        self.assertEqual(result['confirmed_epoch'],50)

    def test_no_recovery_is_reported_when_horizon_is_too_short(self):
        result=cost.recovery(self.snapshots(),0,20,100)
        self.assertIsNone(result['confirmed_epoch'])
        self.assertEqual(result['unconfirmed_low_start_epoch'],0)
        self.assertEqual(result['observed_low_duration_seconds'],20)


if __name__=='__main__':unittest.main()
