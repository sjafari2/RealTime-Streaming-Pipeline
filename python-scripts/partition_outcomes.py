"""Partition rates and evidence checks from the same persisted message identities."""
import json
import math
from pathlib import Path
import sqlite3
import tempfile


def quantiles(db, table, column, where='1', parameters=()):
    # Table/column/where are internal SQL, never command-line input.
    count = db.execute(f'SELECT count({column}) FROM {table} WHERE {where}', parameters).fetchone()[0]
    return {f'p{q}': db.execute(
        f'SELECT {column} FROM {table} WHERE {where} AND {column} IS NOT NULL ORDER BY {column} LIMIT 1 OFFSET ?',
        (*parameters, (q * count + 99) // 100 - 1)).fetchone()[0] if count else None for q in (50, 95, 99)}


def partition_metrics(directory, manifest, latency_sink=None):
    start, end, drain = (manifest[k] for k in ('evaluation_start_epoch', 'producer_end_epoch', 'drain_end_epoch'))
    duration = end - start
    config = manifest['config']
    deadline = float(config.get('SLO_THRESHOLD_MS', 99)) / 1000
    expected = {(f"{config['TOPIC_TITLE']}_{t}", p): t for t in range(int(config.get('TOPIC_COUNT', 0)))
                for p in range(int(config.get('NUM_PARTITIONS', 0)))}
    identity_conflicts = offset_conflicts = ordering_errors = ordering_checked = legacy = missing_callback_times = 0
    failures = []
    with tempfile.TemporaryDirectory() as tmp:
        db = sqlite3.connect(str(Path(tmp) / 'partitions.sqlite'))
        db.executescript('''
          CREATE TABLE identity (id TEXT PRIMARY KEY, topic TEXT, partition INTEGER, offset INTEGER, produced REAL);
          CREATE UNIQUE INDEX physical_record ON identity(topic,partition,offset);
          CREATE TABLE admitted (id TEXT PRIMARY KEY);
          CREATE TABLE arrival (id TEXT PRIMARY KEY);
          CREATE TABLE done (id TEXT PRIMARY KEY, finished REAL);
          CREATE TABLE window_done (id TEXT PRIMARY KEY);
          CREATE TABLE attempts (topic TEXT, partition INTEGER, count INTEGER, PRIMARY KEY(topic,partition));
          CREATE TABLE delays (topic TEXT, partition INTEGER, seconds REAL);
          CREATE INDEX ordered_partition_delays ON delays(topic,partition,seconds);
        ''')
        for path in sorted(Path(directory).rglob('events.jsonl')):
            previous = {}
            with path.open() as stream:
                for line in stream:
                    event = json.loads(line)
                    kind = event.get('event')
                    if kind not in ('acknowledged', 'completed'):
                        continue
                    if event.get('run_id', manifest['run_id']) != manifest['run_id']:
                        failures.append('Message event from another run')
                        continue
                    topic, part, offset = (event.get(k) for k in ('topic', 'partition', 'offset'))
                    if topic is None or part is None or offset is None:
                        legacy += 1
                        continue
                    if (topic, part) not in expected:
                        failures.append('Evidence contains an unexpected topic/partition')
                        continue
                    message = event['message_id']
                    produced = event.get('producer_timestamp')
                    if not isinstance(produced, (float, int)) or not math.isfinite(produced):
                        failures.append('Invalid producer timestamp in partition evidence')
                        continue
                    identity = (topic, part, offset, produced)
                    prior = db.execute('SELECT topic,partition,offset,produced FROM identity WHERE id=?', (message,)).fetchone()
                    if prior and prior != identity:
                        identity_conflicts += 1
                    else:
                        try:
                            db.execute('INSERT OR IGNORE INTO identity VALUES (?,?,?,?,?)', (message, *identity))
                            physical = db.execute('SELECT id FROM identity WHERE topic=? AND partition=? AND offset=?',
                                                  (topic, part, offset)).fetchone()
                            if physical and physical[0] != message:
                                offset_conflicts += 1
                        except sqlite3.IntegrityError:
                            offset_conflicts += 1
                    if kind == 'acknowledged':
                        if start <= produced < end:
                            db.execute('INSERT OR IGNORE INTO admitted VALUES (?)', (message,))
                        if start <= event.get('timestamp', -math.inf) < end:
                            db.execute('INSERT OR IGNORE INTO arrival VALUES (?)', (message,))
                        if not isinstance(event.get('timestamp'), (float, int)):
                            missing_callback_times += 1
                    else:
                        timestamp = event['completion_timestamp']
                        if start <= timestamp < end:
                            db.execute('INSERT OR IGNORE INTO window_done VALUES (?)', (message,))
                            db.execute('INSERT INTO attempts VALUES (?,?,1) ON CONFLICT(topic,partition) DO UPDATE SET count=count+1',
                                       (topic, part))
                        if timestamp <= drain:
                            db.execute('INSERT INTO done VALUES (?,?) ON CONFLICT(id) DO UPDATE SET finished=min(finished,excluded.finished)',
                                       (message, timestamp))
                        epoch = event.get('assignment_epoch')
                        if epoch is not None:
                            key = (topic, part, epoch)
                            if key in previous:
                                ordering_checked += 1
                                # Equal offsets are replay attempts; only a decrease in this ownership epoch is an ordering error.
                                ordering_errors += offset < previous[key]
                            previous[key] = offset
            db.commit()
        rows = []
        for (topic, part), topic_index in sorted(expected.items()):
            params = (topic, part)
            counts = {}
            for table in ('admitted', 'arrival', 'window_done'):
                counts[table] = db.execute(f'SELECT count(*) FROM {table} a JOIN identity i USING(id) WHERE topic=? AND partition=?', params).fetchone()[0]
            complete = late = missing = censored = bad_clock = 0
            for produced, finished in db.execute('''SELECT produced,finished FROM admitted a JOIN identity i USING(id)
                LEFT JOIN done d USING(id) WHERE topic=? AND partition=?''', params):
                if finished is None:
                    missing += 1
                    if produced + deadline <= drain: late += 1
                    else: censored += 1
                else:
                    complete += 1
                    latency = finished - produced
                    if not math.isfinite(latency) or latency < 0:
                        bad_clock += 1
                        continue
                    db.execute('INSERT INTO delays VALUES (?,?,?)', (topic, part, latency))
                    if latency_sink is not None:
                        latency_sink(topic_index, part, latency)
                    late += latency > deadline
            valid, total = db.execute('SELECT count(*),coalesce(sum(seconds),0) FROM delays WHERE topic=? AND partition=?', params).fetchone()
            attempt = db.execute('SELECT count FROM attempts WHERE topic=? AND partition=?', params).fetchone()
            row = dict(topic=topic, topic_index=topic_index, partition=part, admitted_messages=counts['admitted'],
                       completed_cohort_messages=complete, incomplete_messages=missing, deadline_misses=late,
                       deadline_censored=censored, valid_completion_count=valid, completion_latency_sum_seconds=total,
                       completion_mean_seconds=total/valid if valid else None,
                       admitted_per_second=counts['admitted']/duration,
                       acknowledged_arrivals_per_second=counts['arrival']/duration if not missing_callback_times else None,
                       unique_completions_per_second=counts['window_done']/duration,
                       completion_attempts_per_second=(attempt[0] if attempt else 0)/duration,
                       incomplete_fraction=missing/counts['admitted'] if counts['admitted'] else None,
                       deadline_miss_fraction=late/counts['admitted'] if counts['admitted'] and not censored and not bad_clock else None)
            row.update({f'completion_{k}_seconds': v for k, v in quantiles(db, 'delays', 'seconds', 'topic=? AND partition=?', params).items()})
            rows.append(row)
        if identity_conflicts: failures.append('One message identity maps to conflicting record metadata')
        if offset_conflicts: failures.append('One Kafka offset maps to different message identities')
        if ordering_errors: failures.append('Offsets decreased within one consumer ownership epoch')
        if legacy:
            # Older evidence cannot establish partition counts, including zeros.
            rows = []
        return dict(partitions=rows, validity_failures=sorted(set(failures)),
                    correctness=dict(conflicting_message_identities=identity_conflicts,
                                     conflicting_offset_identities=offset_conflicts,
                                     within_epoch_ordering_violations=ordering_errors,
                                     within_epoch_ordering_comparisons=ordering_checked,
                                     events_missing_partition_metadata=legacy,
                                     application_effects_checked=False,
                                     permanent_message_loss_established=False),
                    notes=['Per-partition latency uses the admitted cohort; rates use the common evaluation seconds.',
                           'Acknowledged arrival uses callback time, not exact broker arrival; admission uses producer time.',
                           'Unique completions include warm-up admissions completing in evaluation; attempts include replays.',
                           'Ordering checks are local to a process and ownership epoch; they do not prove ordering at a downstream sink.',
                           'Unfinished by drain does not establish permanent loss; synthetic hashes do not measure durable business effects.',
                           'Partition results are unavailable if legacy outcome events lack partition metadata.'])
