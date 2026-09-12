import json
import math
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'python-scripts'))
from lag_freshness import observation_validity
from analyze_lag import analyze
from test_analysis import make_run
from test_measurements import app, consumer


def metrics(**changes):
    values = dict(consumer_lag=5, consumer_processing_backlog=5, consumer_position_offset=10,
                  consumer_high_offset=15, consumer_lag_valid=1,
                  consumer_lag_observed_timestamp_seconds=100.034,
                  consumer_lag_observation_age_seconds=.5, consumer_lag_scrape_timestamp_seconds=99)
    values.update(changes)
    return values


def test_sample_age_avoids_exporter_wall_clock_and_expires_lookback():
    assert observation_validity(metrics(), 100, 10) == (True, 1.5)
    assert not observation_validity(metrics(), 100, 10, 'legacy_wall_clock_v1')[0]
    assert not observation_validity(metrics(), 110, 10)[0]
    assert not observation_validity(metrics(consumer_lag=math.nan), 100, 10)[0]
    for changes in (dict(consumer_lag_observation_age_seconds=-.1),
                    dict(consumer_lag_scrape_timestamp_seconds=100.01)):
        assert not observation_validity(metrics(**changes), 100, 10)[0]
    missing=metrics();del missing['consumer_lag_observation_age_seconds']
    assert not observation_validity(missing,100,10)[0]


def test_exporter_uses_monotonic_interval_and_rechecks_age_at_scrape(app,monkeypatch):
    clock=SimpleNamespace(value=100.)
    monkeypatch.setattr(consumer,'time',SimpleNamespace(monotonic=lambda:clock.value,time=lambda:-1000.))
    app.update_lag()
    labels=app.partition_labels(('topic_0',0))
    assert consumer.valid_metric.labels(**labels)._value.get()==1
    calls=[]
    original=app.consumer.position
    monkeypatch.setattr(app.consumer,'position',lambda p:(calls.append(1),original(p))[1])
    clock.value=101.
    app.update_lag()
    assert calls==[]  # Negative wall time does not trigger continuous offset queries.
    monkeypatch.setattr(consumer,'generate_latest',lambda:b'')
    clock.value=111.
    app.metrics_snapshot()
    assert consumer.age_metric.labels(**labels)._value.get()==11
    assert consumer.valid_metric.labels(**labels)._value.get()==0
    assert math.isnan(consumer.lag_metric.labels(**labels)._value.get())
    app.update_lag()
    assert consumer.valid_metric.labels(**labels)._value.get()==1


def test_saved_analysis_requires_v2_age_and_scrape_samples(tmp_path):
    root=make_run(tmp_path/'run',values=[10])
    manifest=json.loads((root/'manifest.json').read_text())
    manifest['lag_freshness_clock']='monotonic_scrape_v2'
    (root/'manifest.json').write_text(json.dumps(manifest))
    data=[]
    for p in range(10):
        for name,value in dict(metrics(),consumer_partition_owned=1).items():
            data.append(dict(metric=dict(__name__=name,run_id='r',topic='r_0',partition=str(p),pod='c',incarnation='i'),values=[[100,str(value)],[102,str(value)],[110,str(value)]]))
    path=root/'prometheus.json'
    path.write_text(json.dumps(dict(status='success',data=dict(result=data))))
    result=analyze(root)
    assert result['covered_seconds']==2
    assert result['freshness_clock']=='monotonic_scrape_v2'
    assert not result['snapshots'][-1]['valid']
    data=[row for row in data if row['metric']['__name__']!='consumer_lag_observation_age_seconds']
    path.write_text(json.dumps(dict(status='success',data=dict(result=data))))
    assert analyze(root)['covered_seconds']==0  # Never fall back to wall time for a v2 run.
