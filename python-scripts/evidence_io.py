"""Read the same message evidence before or after lossless compression."""
import gzip
from pathlib import Path


def event_paths(directory):
    directory = Path(directory)
    paths = sorted([*directory.rglob('events.jsonl'), *directory.rglob('events.jsonl.gz')])
    seen = set()
    for path in paths:
        if path.parent in seen:
            raise ValueError('Both plain and compressed evidence exist: ' + str(path.parent))
        seen.add(path.parent)
    return paths


def open_events(path):
    path = Path(path)
    return gzip.open(path, 'rt', encoding='utf-8') if path.suffix == '.gz' else path.open(encoding='utf-8')
