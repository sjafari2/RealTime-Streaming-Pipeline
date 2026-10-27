"""Losslessly compact the redundant CSV export after a run has been validated."""
import argparse
import gzip
import json
import os
from pathlib import Path
import tempfile

from compress_evidence import checksum


def compress_csv(directory):
    directory = Path(directory)
    status = json.loads((directory / 'runner-status.json').read_text())
    manifest = json.loads((directory / 'manifest.json').read_text())
    if status.get('status') != 'complete' or status.get('run_id') != manifest['run_id']:
        raise ValueError('Only a completed, validated export can be compressed')
    source = directory / 'prometheus.csv'
    destination = directory / 'prometheus.csv.gz'
    if not source.exists():
        if destination.exists() and (directory / 'compressed-monitoring-csv.json').exists():
            return json.loads((directory / 'compressed-monitoring-csv.json').read_text())
        raise ValueError('Missing monitoring CSV')
    if destination.exists():
        raise ValueError('Inspect the existing compressed CSV before replacing anything')
    before = source.stat()
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=directory, prefix='.monitoring-', delete=False) as out:
            temporary = Path(out.name)
            with source.open('rb') as stream:
                original_hash = checksum(stream)
            with source.open('rb') as stream, gzip.GzipFile(filename='', fileobj=out, mode='wb', mtime=0) as zipped:
                for block in iter(lambda: stream.read(1024*1024), b''):
                    zipped.write(block)
            out.flush()
            os.fsync(out.fileno())
        with gzip.open(temporary, 'rb') as stream:
            if checksum(stream) != original_hash:
                raise ValueError('Compressed CSV did not reproduce the original bytes')
        after = source.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError('CSV changed during compression')
        with temporary.open('rb') as stream:
            compressed_hash = checksum(stream)
        os.replace(temporary, destination)
        temporary = None
        record = dict(original_path=source.name, compressed_path=destination.name,
                      original_sha256=original_hash, compressed_sha256=compressed_hash,
                      original_bytes=before.st_size, compressed_bytes=destination.stat().st_size)
        (directory / 'compressed-monitoring-csv.json').write_text(json.dumps(record, indent=2) + '\n')
        source.unlink()
        return record
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    args = parser.parse_args()
    print(json.dumps(compress_csv(args.run_directory), indent=2))
