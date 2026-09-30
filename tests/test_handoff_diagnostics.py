"""Check event-based counts at ACK and completion boundaries."""
from pathlib import Path
import sys
import numpy as np
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python-scripts'))
from draw_handoff_diagnostics import outstanding_counts


def test_outstanding_includes_warmup_and_handles_completion_before_ack():
    # warm-up is acknowledged before evaluation; one message never completes.
    ack={'warmup':-1,'normal':1,'early_completion':4,'unfinished':3}
    done={'warmup':2,'normal':3,'early_completion':2}
    times=np.array([0,1,2,3,4,5])
    assert outstanding_counts(ack,done,times).tolist()==[1,2,1,1,1,1]


def test_unmatched_completion_fails_instead_of_lowering_backlog():
    with pytest.raises(ValueError,match='Completion without'):
        outstanding_counts({'one':0},{'other':1},np.array([0,2]))
