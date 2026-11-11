import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from analyze_repetitions import ARMS, CONDITIONS, MATCHED_FIELDS, validate_campaign, validate_protocols, reviewed_attempts


def campaign(repetitions):
    order = [(c+'__'+a,n,80+n) for n in repetitions for a in ARMS for c in CONDITIONS]
    return dict(status='complete', restoration='verified',
                cost_protocol={'order':order,'matched_workloads':{k:'same' for k in MATCHED_FIELDS}},
                runs=[dict(arm=a,run_number=n,seed=s,run_id=str(i),validation={'status':'passed'}) for i,(a,n,s) in enumerate(order)])


class CampaignValidationTests(unittest.TestCase):
    def test_additional_eight_runs_are_accepted(self):
        validate_campaign(campaign([3]), [3])

    def test_partial_and_unrestored_runs_are_rejected(self):
        for field,value in [('status','running'),('restoration','pending')]:
            c=campaign([3]);c[field]=value
            with self.assertRaises(ValueError):validate_campaign(c,[3])
        c=campaign([3]);c['runs'].pop()
        with self.assertRaises(ValueError):validate_campaign(c,[3])

    def test_failed_outcomes_cannot_enter_a_completed_group(self):
        c=campaign([3]);c['runs'][0]['validation']['status']='failed'
        with self.assertRaises(ValueError):validate_campaign(c,[3])

    def test_reused_run_identity_is_rejected(self):
        c=campaign([3]);c['runs'][0]['run_id']=c['runs'][1]['run_id']
        with self.assertRaises(ValueError):validate_campaign(c,[3])

    def test_different_repetition_counts_do_not_change_comparability(self):
        validate_protocols(campaign([1,2]),campaign([3]))

    def test_changed_deployment_or_measurement_protocol_is_rejected(self):
        for field in MATCHED_FIELDS:
            before=campaign([1,2]);after=campaign([3])
            after['cost_protocol']['matched_workloads'][field]='different'
            with self.assertRaises(ValueError):validate_protocols(before,after)

    def test_excluded_attempt_cannot_also_be_pooled(self):
        c=campaign([3])
        c['technical_attempts']=[dict(trial=c['runs'][0],classification='runner_delay',retention='Raw evidence retained')]
        with self.assertRaisesRegex(ValueError,'also an accepted'):
            reviewed_attempts(c)

    def test_technical_exclusion_needs_a_documented_reason(self):
        c=campaign([3])
        c['technical_attempts']=[dict(trial={'run_id':'excluded'})]
        with self.assertRaisesRegex(ValueError,'review or retention'):
            reviewed_attempts(c)

    def test_incomplete_attempt_retains_verified_raw_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / 'raw'
            backup = root / 'backup'
            raw.mkdir()
            backup.mkdir()
            (raw / 'events.jsonl').write_bytes(b'original evidence\n')
            (backup / 'backup-sha256.json').write_text(json.dumps({
                'events.jsonl': hashlib.sha256(b'original evidence\n').hexdigest()}))
            c = campaign([3])
            c['technical_attempts'] = [dict(
                trial=dict(run_id='excluded', directory=str(raw)),
                classification='incomplete_execution',
                retention=dict(verified_backup=str(backup)))]
            self.assertEqual(len(reviewed_attempts(c)), 1)
            (raw / 'events.jsonl').write_bytes(b'changed evidence\n')
            with self.assertRaisesRegex(ValueError, 'evidence changed'):
                reviewed_attempts(c)


if __name__=='__main__':unittest.main()
