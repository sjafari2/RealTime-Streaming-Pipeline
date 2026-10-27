"""Check that different preparation histories retain the same future input trace."""
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'python-scripts'))
from evidence_io import event_paths, open_events


def evaluation_traces(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    traces = {}
    for path in event_paths(directory / 'producer'):
        records = {}
        with open_events(path) as stream:
            for line in stream:
                row = json.loads(line)
                if row['event'] != 'acknowledged':
                    continue
                produced = row['producer_timestamp']
                if not manifest['evaluation_start_epoch'] <= produced < manifest['producer_end_epoch']:
                    continue
                index = row.get('workload_phase_index')
                if row.get('workload_phase') != 1 or type(index) is not int or index < 0 or index in records:
                    raise ValueError('Evaluation trace has invalid or duplicate phase indices')
                records[index] = row['partition']
        if not records or sorted(records) != list(range(len(records))):
            raise ValueError('Evaluation trace has missing acknowledgments or phase indices')
        pod = path.parent.parent.name
        if pod in traces:
            raise ValueError('Multiple producer incarnations in a preparation')
        traces[pod] = [records[i] for i in range(len(records))]
    if set(traces) != set(manifest['producer_pods']):
        raise ValueError('Missing producer evaluation trace')
    return traces


def compare_traces(smaller, larger):
    left, right = evaluation_traces(Path(smaller)), evaluation_traces(Path(larger))
    if set(left) != set(right):
        raise ValueError('Different producers in the two preparations')
    result = {}
    for pod in left:
        count = min(len(left[pod]), len(right[pod]))
        if abs(len(left[pod]) - len(right[pod])) > max(2, .001 * count):
            raise ValueError('Evaluation input counts differ by more than the trace tolerance')
        if left[pod][:count] != right[pod][:count]:
            raise ValueError('Evaluation partition sequence depends on preparation history')
        digest = hashlib.sha256(json.dumps(left[pod][:count], separators=(',', ':')).encode()).hexdigest()
        result[pod] = dict(compared_messages=count, smaller_messages=len(left[pod]),
                           larger_messages=len(right[pod]), common_sequence_sha256=digest)
    return dict(status='passed', producers=result,
                definition='The same seed and logical producer produce the same evaluation partition sequence; arrival timestamps may differ.')
