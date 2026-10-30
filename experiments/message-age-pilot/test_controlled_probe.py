import unittest
from controlled_age_probe import busy_capacity


class BusyCalibrationChecks(unittest.TestCase):
    def setUp(self):
        self.records=[['x',0,i,0,i/100,(i+1)/100,.01,'calibration'] for i in range(8000)]
        self.resources=[[i,100*i,0,0,0] for i in range(81)]

    def test_wall_clock_rate_includes_service_and_uses_sixty_seconds(self):
        result=busy_capacity(self.records,self.resources,0,20000)
        self.assertTrue(result['eligible'])
        self.assertEqual(result['effective_rate'],100)
        self.assertEqual(result['conservative_rate'],85)

    def test_depleted_queue_cannot_establish_capacity(self):
        result=busy_capacity(self.records[:6500],self.resources,0,6500)
        self.assertFalse(result['eligible'])

    def test_empty_poll_disqualifies_interval(self):
        for row in self.resources[40:]:row[-1]=1
        self.assertFalse(busy_capacity(self.records,self.resources,0,20000)['eligible'])

    def test_processing_pause_disqualifies_interval(self):
        records=[r for r in self.records if not 40<r[4]<41]
        self.assertFalse(busy_capacity(records,self.resources,0,20000)['eligible'])


if __name__=='__main__':unittest.main()
