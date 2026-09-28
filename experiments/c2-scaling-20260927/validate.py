"""Audit the live handoff against original acknowledgments and completions."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'python-scripts'))
from evidence_io import event_paths,open_events


def validate(directory, require_all=False):
    directory=Path(directory)
    manifest=json.loads((directory/'manifest.json').read_text())
    outcome=json.loads((directory/'outcome-summary.json').read_text())
    failures=list(outcome['validity_failures'])
    if outcome['failed_or_cancelled_sends_whole_run'] or outcome['unresolved_sends_whole_run']:
        failures.append('Producer admission was incomplete')
    handoff=manifest['intervention']['action'] in ('redistribute','scale_redistribute')
    transitions=[]
    if handoff:
        transitions=[json.loads(line) for line in (directory/'explicit-handoff-events.jsonl').read_text().splitlines()]
        expected=['release_requested','released_verified','acquire_requested','acquired_verified','resume_requested','active_verified']
        if [e['event'] for e in transitions]!=expected:
            failures.append('Handoff barriers are incomplete or repeated')
        if manifest['intervention']['action']=='scale_redistribute' and manifest.get('explicit_scale_result',{}).get('status')!='resumed':
            failures.append('Scale handoff did not resume')
        if require_all and transitions:
            lag=json.loads((directory/'lag-summary.json').read_text())
            queued=[x['processing_backlog'] for x in lag['snapshots']
                    if x.get('valid') and x['timestamp'] < transitions[0]['timestamp']]
            if not queued or max(queued) <= 100:
                failures.append('Technical gate did not demonstrate queued work before handoff')
    with tempfile.TemporaryDirectory() as tmp:
        db=sqlite3.connect(str(Path(tmp)/'handoff.sqlite'))
        db.executescript('''CREATE TABLE ack(id TEXT PRIMARY KEY, p INTEGER, o INTEGER);
            CREATE TABLE done(id TEXT, p INTEGER, o INTEGER, pod TEXT, t REAL);
            CREATE INDEX done_partition ON done(p,o);''')
        for path in event_paths(directory):
            final=json.loads(path.with_name('final.json').read_text())
            with open_events(path) as stream:
                for line in stream:
                    e=json.loads(line)
                    if e['event']=='acknowledged':
                        db.execute('INSERT OR IGNORE INTO ack VALUES(?,?,?)',(e['message_id'],e['partition'],e['offset']))
                    elif e['event']=='completed':
                        db.execute('INSERT INTO done VALUES(?,?,?,?,?)',(e['message_id'],e['partition'],e['offset'],final['pod'],e['completion_timestamp']))
        db.commit()
        acknowledged=db.execute('SELECT count(*) FROM ack').fetchone()[0]
        completed=db.execute('SELECT count(*) FROM done').fetchone()[0]
        duplicate_ids=db.execute('SELECT count(*) FROM (SELECT id FROM done GROUP BY id HAVING count(*)>1)').fetchone()[0]
        duplicate_offsets=db.execute('SELECT count(*) FROM (SELECT p,o FROM done GROUP BY p,o HAVING count(*)>1)').fetchone()[0]
        unknown=db.execute('SELECT count(*) FROM done d LEFT JOIN ack a ON a.id=d.id AND a.p=d.p AND a.o=d.o WHERE a.id IS NULL').fetchone()[0]
        missing=db.execute('SELECT count(*) FROM ack a LEFT JOIN done d ON d.id=a.id WHERE d.id IS NULL').fetchone()[0]
        if duplicate_ids or duplicate_offsets or unknown:
            failures.append('Duplicate completion or acknowledgment identity mismatch')
        if require_all and missing:
            failures.append('Technical validation did not finish all acknowledged messages')
        # Fresh topics start at zero. Any hole within a completed prefix is invalid,
        # even if normal performance trials legitimately end with unfinished tails.
        for p,n,lo,hi in db.execute('SELECT p,count(DISTINCT o),min(o),max(o) FROM done GROUP BY p'):
            if lo!=0 or n!=hi+1:failures.append('Noncontiguous completed offsets in partition '+str(p))
        if handoff and len(transitions)==6:
            released=transitions[1]; acquired=transitions[3]
            command=transitions[2]['command']
            target={x['partition']:x['owner'] for x in command['ownership']}
            for name,row in released['statuses'].items():
                for part,offset in row['explicit_assignment']['offsets'].items():
                    p=int(part)
                    before=db.execute('SELECT count(*),max(o) FROM done WHERE p=? AND t<=?',(p,released['timestamp'])).fetchone()
                    if before[0]!=offset or (offset and before[1]!=offset-1):
                        failures.append('Released frontier differs from completed prefix: '+part)
                    after=db.execute('SELECT min(o),count(DISTINCT pod),min(pod) FROM done WHERE p=? AND t>?',(p,released['timestamp'])).fetchone()
                    if after[0] is not None and (after[0]!=offset or after[1]!=1 or after[2]!=target[p]):
                        failures.append('Destination did not resume at verified offset: '+part)
            during=db.execute('SELECT count(*) FROM done WHERE t>? AND t<?',(released['timestamp'],transitions[4]['timestamp'])).fetchone()[0]
            if during:failures.append('Completion occurred before the resume barrier')
    result=dict(run_id=manifest['run_id'],status='failed' if failures else 'passed',failures=failures,
        acknowledged_whole_run=acknowledged,completion_attempts_whole_run=completed,
        unfinished_whole_run=missing,duplicate_message_ids=duplicate_ids,duplicate_offsets=duplicate_offsets,
        unmatched_completion_identities=unknown,handoff_checked=handoff,
        interpretation='Synthetic completion/offset evidence only; not exactly-once external application effects.')
    (directory/'handoff-validation.json').write_text(json.dumps(result,indent=2)+'\n')
    if failures:raise RuntimeError('; '.join(failures))
    return result
