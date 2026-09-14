"""Check offline planning and transfer contracts before any Kafka adapter exists."""
import importlib.util
from pathlib import Path
import copy
import pytest

ROOT = Path(__file__).resolve().parents[1] / 'experiments/partition-reassignment-pilot'
def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

planner, handoff = load('planner'), load('handoff')


def snapshot(rates=(60, 60, 60, 10, 10, 10), owners=('a', 'a', 'b', 'b', 'c', 'c'), cap=100):
    return dict(schema_version=1, work_model='equal_cost_records', run_id='test', assignment_epoch=0,
                observed_at=100, maximum_age_seconds=10, capacity_observed_at=90,
                capacity_maximum_age_seconds=20, capacity_records_per_second=dict(a=cap, b=cap, c=cap),
                expected_partitions=list(range(len(rates))),
                partitions=[dict(partition=i, owner=c, arrival_records_per_second=r,
                                 processing_backlog=100, valid=True, observed_at=100)
                            for i, (r, c) in enumerate(zip(rates, owners))])


def test_hot_partitions_already_on_different_consumers():
    s = snapshot(); before = copy.deepcopy(s)
    p = planner.plan(s)
    assert p['status'] == 'candidate' and not p['executable']
    assert p['before_utilization'] == {'a': 1.2, 'b': .7, 'c': .2}
    assert max(p['after_utilization'].values()) <= .85
    assert all(m['partition'] != 2 for m in p['moves'])  # b already owns its hot partition
    assert len({m['partition'] for m in p['moves']}) == len(p['moves'])
    assert s == before


def test_balanced_ownership_keeps_assignment():
    p = planner.plan(snapshot(owners=('a', 'b', 'c', 'a', 'b', 'c')))
    assert p['status'] == 'no_action' and p['moves'] == []


def test_group_overload_is_not_solved_by_redistribution():
    p = planner.plan(snapshot(cap=50))
    assert p['status'] == 'infeasible' and p['moves'] == []


def test_indivisible_partition_never_gets_split():
    p = planner.plan(snapshot(rates=(120, 5, 5, 5, 5, 5)))
    assert p['status'] == 'no_feasible_plan' and p['moves'] == []


def test_heterogeneous_capacity_changes_feasibility():
    s = snapshot(rates=(120, 5, 5, 5, 5, 5)); s['capacity_records_per_second']['c'] = 200
    assert planner.plan(s)['status'] == 'candidate'


@pytest.mark.parametrize('change', ['missing','duplicate','stale','future','invalid','nan','capacity','cost'])
def test_bad_evidence_is_rejected(change):
    s = snapshot()
    if change == 'missing': s['partitions'].pop()
    if change == 'duplicate': s['partitions'].append(copy.deepcopy(s['partitions'][0]))
    if change == 'stale': s['partitions'][0]['observed_at'] = 89
    if change == 'future': s['partitions'][0]['observed_at'] = 101
    if change == 'invalid': s['partitions'][0]['valid'] = False
    if change == 'nan': s['partitions'][0]['arrival_records_per_second'] = float('nan')
    if change == 'capacity': s['capacity_observed_at'] = 70
    if change == 'cost': s['work_model'] = 'variable_cost_records'
    with pytest.raises(ValueError): planner.plan(s)


def test_change_budget_does_not_release_partial_plan():
    s=snapshot(rates=(45,45,45,45,0,0),owners=('a','a','a','a','b','c'))
    p=planner.plan(s,max_changed_partitions=1)
    assert p['status'] == 'no_feasible_plan' and p['moves'] == []


def transfer():
    return handoff.Handoff('r', 4, {0:'a',1:'b',2:'a',3:'c'},
        [dict(partition=0,source='a',destination='c'),dict(partition=3,source='c',destination='a')],
        dict(a='a1',b='b1',c='c1'))


def released(p,owner,offset=8):
    return dict(run_id='r',epoch=4,partition=p,owner=owner,incarnation=owner+'1',
                processing_stopped=True,unassigned=True,commit_verified=True,evidence_flushed=True,
                next_offset=offset,committed_offset=offset)


def acquired(p,owner,offset=8):
    return dict(run_id='r',epoch=4,partition=p,owner=owner,incarnation=owner+'1',
                processing_paused=True,next_offset=offset)


def test_swap_has_release_barrier_then_acquire_barrier():
    h=transfer();h.release(released(0,'a'))
    with pytest.raises(handoff.InvalidHandoff):h.acquire(acquired(0,'c'))
    h.release(released(3,'c'));h.acquire(acquired(0,'c'))
    with pytest.raises(handoff.InvalidHandoff):h.resume('r',4,h.ownership,h.incarnations)
    h.acquire(acquired(3,'a'));h.resume('r',4,{0:'c',1:'b',2:'a',3:'a'},dict(a='a1',b='b1',c='c1'))
    assert h.phase == 'resumed'


@pytest.mark.parametrize('field,value',[('epoch',3),('run_id','old'),('incarnation','a2'),
    ('unassigned',False),('commit_verified',False),('evidence_flushed',False),('processing_stopped',False),
    ('committed_offset',9),('next_offset',-1)])
def test_release_cannot_be_assumed(field,value):
    h=transfer();row=released(0,'a');row[field]=value
    with pytest.raises(handoff.InvalidHandoff):h.release(row)
    assert h.ownership[0]=='a' and not h.released


def test_destination_cannot_skip_unfinished_work_or_resume_early():
    h=transfer();h.release(released(0,'a'));h.release(released(3,'c'))
    with pytest.raises(handoff.InvalidHandoff):h.acquire(acquired(0,'c',9))
    row=acquired(0,'c');row['processing_paused']=False
    with pytest.raises(handoff.InvalidHandoff):h.acquire(row)


def test_timeout_never_grants_new_ownership():
    h=transfer();h.abort('source did not respond')
    with pytest.raises(handoff.InvalidHandoff):h.release(released(0,'a'))
    with pytest.raises(handoff.InvalidHandoff):h.acquire(acquired(0,'c'))
    assert h.ownership[0]=='a'


def test_restart_before_resume_invalidates_handoff():
    h=transfer();h.release(released(0,'a'));h.release(released(3,'c'))
    h.acquire(acquired(0,'c'));h.acquire(acquired(3,'a'))
    with pytest.raises(handoff.InvalidHandoff):h.resume('r',4,h.ownership,dict(a='a2',b='b1',c='c1'))


def test_swap_can_work_when_neither_simple_move_fits():
    s = snapshot(rates=(60,50,30,20), owners=('a','a','b','b'))
    s['capacity_records_per_second'].pop('c')
    p=planner.plan(s)
    assert p['status']=='candidate' and len(p['moves'])==2
    assert p['after_utilization']=={'a':.8,'b':.8}


def test_zero_is_a_valid_resume_offset_but_boolean_is_not():
    h=transfer();h.release(released(0,'a',0));h.release(released(3,'c',0))
    row=acquired(0,'c',False)
    with pytest.raises(handoff.InvalidHandoff):h.acquire(row)
    h.acquire(acquired(0,'c',0));h.acquire(acquired(3,'a',0))
    assert h.phase=='ready_to_resume'


def test_candidates_preserve_every_partition_and_total_work():
    import random
    rng=random.Random(14)
    for _ in range(60):
        s=snapshot(rates=[rng.randint(0,70) for i in range(6)],
                   owners=[rng.choice('abc') for i in range(6)])
        p=planner.plan(s)
        if p['status']!='candidate':
            assert p['moves']==[]
            continue
        owners={r['partition']:r['owner'] for r in s['partitions']}
        for m in p['moves']:
            assert owners[m['partition']]==m['source']
            owners[m['partition']]=m['destination']
        rows=[dict(r,owner=owners[r['partition']]) for r in s['partitions']]
        assert len(owners)==6 and max(planner.loads(rows,s['capacity_records_per_second']).values())<=.85
        assert sum(r['arrival_records_per_second'] for r in rows)==sum(r['arrival_records_per_second'] for r in s['partitions'])
