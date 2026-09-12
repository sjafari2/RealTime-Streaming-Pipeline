import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'python-scripts'))
import compare_runs as comparison


def test_processing_area_clips_the_horizon_without_filling_gaps():
    rows = [dict(timestamp=t, valid=valid, processing_backlog=back, growth_offsets_per_second=growth)
            for t,valid,back,growth in [(0,True,0,None), (2,True,10,5), (4,True,20,5), (6,False,None,None)]]
    result = comparison.processing_integral(rows, 1, 5)
    assert result['area_offset_seconds'] == pytest.approx(37.5)
    assert result['covered_seconds'] == 3
    assert result['covered_fraction'] == .75
    rows[2]['growth_offsets_per_second'] = None  # Ownership/offset discontinuity rejected upstream.
    result = comparison.processing_integral(rows, 1, 5)
    assert result['area_offset_seconds'] == pytest.approx(7.5)
    assert result['covered_seconds'] == 1


def test_monitoring_loss_does_not_hide_an_unfavorable_valid_message_outcome(monkeypatch):
    def run(path):
        action = 'scale' if 'scale' in path.name else 'none'
        bad = 'bad' in path.name
        return dict(run_id=path.name, config={'WORKLOAD_SEED':'1'}, action=action,
                    source_signatures=['known'], intervention={}, quality_flags=['coverage'] if bad else [],
                    metrics={'p99_seconds': (14 if bad else 7) if action=='scale' else 10, 'unfinished_fraction':0.0,
                             'lag_covered_fraction':.85 if bad else .98, 'mean_processing_backlog_offsets':100,
                             'evaluation_and_drain_resource_coverage':.8 if bad else .99,
                             'evaluation_and_drain_requested_cpu_seconds':100})
    monkeypatch.setattr(comparison, 'run_record', run)
    index = dict(pairs=[dict(pair=1,none='none-good',scale='scale-good'),
                        dict(pair=2,none='none-bad',scale='scale-bad')])
    result = comparison.summarize(index, Path('/unused'))
    assert len(result['pairs']) == 2
    assert result['pairs'][1]['runs']['scale']['quality_flags'] == ['coverage']
    assert result['descriptive_paired_differences']['p99_seconds'] == dict(
        pair_count=2,mean_paired_difference=.5,minimum=-3,maximum=4,sample_standard_deviation=pytest.approx(4.949747))
    assert result['descriptive_paired_differences']['mean_processing_backlog_offsets']['pair_count'] == 1
    assert result['descriptive_paired_differences']['evaluation_and_drain_requested_cpu_seconds']['pair_count'] == 1


def test_short_and_sustained_pairs_cannot_be_pooled(monkeypatch):
    def run(path):
        action = 'scale' if 'scale' in path.name else 'none'
        return dict(run_id=path.name, config={'EXP_DURATION_SEC':'600' if 'long' in path.name else '180'},
                    action=action, source_signatures=['known'], intervention={}, quality_flags=[], metrics={'p99_seconds':1})
    monkeypatch.setattr(comparison, 'run_record', run)
    index = dict(pairs=[dict(pair=1,none='none-long',scale='scale-long'),
                        dict(pair=2,none='none-short',scale='scale-short')])
    with pytest.raises(ValueError, match='separate comparison index'):
        comparison.summarize(index, Path('/unused'))


def test_incompatible_pair_is_retained_with_its_reason(monkeypatch):
    def run(path):
        action = path.name
        return dict(run_id=action, config={'EXP_DURATION_SEC':'600' if action=='scale' else '180'},
                    action=action, source_signatures=['known'], intervention={}, quality_flags=[], metrics={'p99_seconds':1})
    monkeypatch.setattr(comparison, 'run_record', run)
    result = comparison.summarize(dict(pairs=[dict(pair=1,none='none',scale='scale')]), Path('/unused'))
    assert result['pairs'][0]['compatibility_failures'] == ['Frozen workload configurations differ']
    assert result['descriptive_paired_differences'] == {}
