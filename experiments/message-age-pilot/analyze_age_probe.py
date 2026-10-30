"""Reconcile pilot records independently and compare saved pre-release predictions."""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import tarfile


def quantile(values):
    return sorted(values)[math.ceil(.99*len(values))-1] if values else None


def analyze(directory, output):
    execution=json.loads((directory/'execution.json').read_text())
    archive=directory/'raw-evidence.tar.gz'
    digest=hashlib.sha256(archive.read_bytes()).hexdigest()
    assert digest==execution['archive_sha256']
    extracted=directory/'extracted'; extracted.mkdir(exist_ok=True)
    with tarfile.open(archive) as tf:
        for member in tf.getmembers():
            target=(extracted/member.name).resolve()
            assert os.path.commonpath([str(target),str(extracted.resolve())])==str(extracted.resolve())
            assert member.isfile() or member.isdir()
        tf.extractall(extracted)
    root=extracted/execution['token']
    saved=json.loads((root/'summary.json').read_text())
    decision=json.loads((root/'decision.json').read_text())
    clockfile=directory/'clock-probe.json'
    clocks=json.loads(clockfile.read_text()) if clockfile.exists() else execution['clock_bounds']
    clocks={c['pod']:c for c in clocks}; pc=clocks['producer-workers-0']
    rows=[]; snapshots=[]; sample_ages={}
    for summary in saved:
        if summary['calibration']: continue
        episode=root/summary['id']
        manifest=json.loads((episode/'manifest.json').read_text())
        producer=json.loads((episode/'producer.json').read_text())
        before=json.loads((episode/'producer-before.json').read_text())
        release=producer['release_epoch']; cutoff=release+manifest['observation_seconds']
        acknowledgments={r[0]:r for r in producer['ack']}
        assert len(acknowledgments)==len(producer['ack']) and not producer['errors']
        initial={key:r for key,r in acknowledgments.items() if r[5]!='continuing'}
        assert initial.keys()=={r[0] for r in before['ack']}
        assert all(r[4]<=release for r in initial.values())
        parts=Counter(r[1] for r in initial.values()); snapshots.append([parts[p] for p in range(60)])
        completed={}; observed=[]; certain_late=0; possible_late=0
        for c in range(6):
            data=json.loads((episode/f'consumer-{c}.json').read_text()); assert data['error'] is None
            offset_by_part={}; previous_end=None
            for r in data['records']:
                ident,part,offset,created,start,end,service,cohort=r
                assert ident in acknowledgments and ident not in completed
                assert manifest['owners'][part]==c and service>=0 and end>=start
                assert offset==offset_by_part.get(part,-1)+1
                assert previous_end is None or start>=previous_end-1e-6
                offset_by_part[part]=offset;previous_end=end
                assert acknowledgments[ident][1:4]==[part,offset,created]
                completed[ident]=r
                if ident in initial and end<=cutoff:
                    observed.append(r)
                    latency=end-created;cc=clocks[f'consumer-sts-{c}']
                    lower=latency-(cc['maximum_offset_seconds']-pc['minimum_offset_seconds'])
                    upper=latency-(cc['minimum_offset_seconds']-pc['maximum_offset_seconds'])
                    certain_late+=lower>20;possible_late+=upper>20
        initial_done={r[0] for r in observed};missing=len(initial)-len(initial_done)
        latency=[r[5]-r[3] for r in observed];p99=quantile(latency)
        late=sum(x>20 for x in latency)+missing
        # At a 30-second cutoff every initial message has already crossed its
        # 20-second deadline, so unfinished messages are known violations here.
        certain_late+=missing;possible_late+=missing
        assert missing==summary['initial_unfinished']
        assert abs(p99-summary['initial_completion_p99'])<1e-7
        assert late==summary['deadline_misses']['20']
        predictions=json.loads((episode/'predictions.json').read_text())
        assert predictions['at_epoch']<=release
        arm=predictions['observed_response']
        selected=next(p for p in predictions['candidates'] if p['response']==arm)
        allowed=len(initial)-math.ceil(.99*len(initial))
        observed_pass=late<=allowed
        certain='pass' if possible_late<=allowed else ('fail' if certain_late>allowed else 'clock-ambiguous')
        initial_ages=[release-r[3] for r in initial.values()]
        preload_ages=[release-r[3] for r in initial.values() if r[5]=='preload']
        sample_ages[summary['id']]=sorted(preload_ages)
        rate_inputs=json.loads((episode/'decision-inputs.json').read_text())
        row=dict(id=summary['id'],response=arm,run=int(summary['id'].rsplit('run',1)[1]),
            added_hold_seconds=summary['hold_seconds'],initial_count=len(initial),
            completed=len(initial_done),unfinished=missing,completion_p99_seconds=p99,
            age_p99_seconds=quantile(initial_ages),preload_age_min=min(preload_ages),preload_age_max=max(preload_ages),
            deadline_misses_20s=late,deadline_miss_percent=100*late/len(initial),
            meets_99_percent_within_20s=observed_pass,clock_robust_classification=certain,
            predicted_age_blind_p99=selected['p99_age_blind'],predicted_age_aware_p99=selected['p99_age_aware'],
            blind_false_success=selected['p99_age_blind']<=20 and not observed_pass,
            aware_false_success=selected['p99_age_aware']<=20 and not observed_pass,
            measured_total_arrival_rate=sum(rate_inputs['measured_rates']),
            planned_consumers=decision['rule']['consumers'] if arm=='baseline' else 6,
            active_consumers=summary['active_consumers'],allocated_pods=6)
        rows.append(row)
    assert len(rows)==8 and len({tuple(x) for x in snapshots})==1
    original={r['pod']:(r['uid'],r['node']) for r in execution['before'] if int(r['pod'].rsplit('-',1)[1])<execution['original_replicas']}
    final={r['pod']:(r['uid'],r['node']) for r in execution['after']}
    assert all(original[p]==final[p] for p in original)
    result=dict(kind='isolated real-Kafka queue-age pilot',rows=rows,rule=decision,
        archive_sha256=digest,remote_raw_evidence=execution['remote'],
        validation=dict(matched_queue_counts=True,cohort_per_partition=snapshots[0],
            distinct_ids_and_offset_continuity=True,stored_predictions_precede_release=True,
            original_pods_unchanged=True,restored_replicas=execution['restored_replicas'],
            episodes=len(rows),clock_robust_outcomes=sum(r['clock_robust_classification']!='clock-ambiguous' for r in rows)),
        prediction_checks=dict(age_blind_false_successes=sum(r['blind_false_success'] for r in rows),
                               age_aware_false_successes=sum(r['aware_false_success'] for r in rows)),
        limits=['Explicit final assignments and prestarted pods; no measured scale-out or handover delay.',
            'All six pods allocated throughout; no measured resource-cost reduction.',
            'Paper-based reconstruction; not the authors exact released controller.',
            'Pooled task-processing rate and oldest-first prediction are assumptions, not a calibrated Kafka service model.',
            'The 20-second diagnostic objective concerns the waiting cohort; it does not replace the proposal SLO.',
            'Two repetitions do not establish reproducibility or novelty.'])
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps(result['validation']))
    for r in rows:
        print(r['id'],round(r['completion_p99_seconds'],3),r['unfinished'],
              round(r['predicted_age_blind_p99'],3),round(r['predicted_age_aware_p99'],3),r['clock_robust_classification'])
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args();analyze(a.directory,a.output)
