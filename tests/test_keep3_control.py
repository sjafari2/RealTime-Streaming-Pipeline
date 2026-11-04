"""A standalone stress control must retain three consumers and the declared owner."""
import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
SOURCE=Path(os.environ.get('KEEP3_SCRIPT',ROOT/'experiments/twelve-hot600-20261104/run_keep3.py'))
spec=importlib.util.spec_from_file_location('keep3_control',SOURCE)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def plan():
    return dict(schema_version=1,purpose='single_keep3_new_deployment',hot_owner='consumer-sts-0',
                aggregate_rate=600,phases_seconds=[60,600,120],maximum_performance_trials=1,
                continue_to_mitigation=False)


def test_single_control_is_accepted():
    module.validate_plan(plan())


@pytest.mark.parametrize('change',[{'hot_owner':'consumer-sts-2'},{'maximum_performance_trials':8},
                                  {'continue_to_mitigation':True},{'aggregate_rate':700},
                                  {'purpose':'matched_comparison'}])
def test_changes_to_the_authorized_trial_are_rejected(change):
    with pytest.raises(ValueError):module.validate_plan(dict(plan(),**change))


def test_hot_set_comes_only_from_consumer_zero():
    reference={'assignment':[dict(topic_index=0,partition=p,pod='consumer-sts-'+str((p//20+2)%3))
                             for p in range(60)]}
    assert module.hot_partitions(reference)==list(range(20,32))
    reference['assignment']=reference['assignment'][:20]
    with pytest.raises(ValueError):module.hot_partitions(reference)
