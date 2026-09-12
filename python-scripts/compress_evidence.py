"""Compress closed local message logs without changing or discarding their contents."""
import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from evidence_io import event_paths


def checksum(stream):
    digest = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b''):
        digest.update(block)
    return digest.hexdigest()


def compress(directory):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / 'manifest.json').read_text())
    status = json.loads((directory / 'runner-status.json').read_text())
    if status.get('status') != 'complete' or status.get('run_id') != manifest['run_id']:
        raise ValueError('Compress only a completed, validated managed run')
    if time.time() <= manifest['drain_end_epoch']:
        raise ValueError('The run has not finished its drain')
    paths = event_paths(directory)
    if not paths:
        raise ValueError('No message evidence was found')
    for path in paths:
        final = json.loads(path.with_name('final.json').read_text())
        if final.get('run_id') != manifest['run_id'] or final.get('failure') or final.get('evidence_dropped') or final.get('evidence_error'):
            raise ValueError('Inspect failed or mismatched process evidence before compression')
    report_path = directory / 'compressed-evidence.json'
    report = json.loads(report_path.read_text()) if report_path.exists() else dict(run_id=manifest['run_id'], files=[])
    for path in paths:
        if path.suffix == '.gz':
            continue
        before = path.stat()
        destination = path.with_name(path.name + '.gz')
        if destination.exists():
            raise ValueError('A compressed file already exists: ' + str(destination))
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.compress-', delete=False) as output:
                temporary = Path(output.name)
                digest = hashlib.sha256()
                with path.open('rb') as source, gzip.GzipFile(filename='', mode='wb', fileobj=output, compresslevel=6, mtime=0) as archive:
                    for block in iter(lambda: source.read(1024 * 1024), b''):
                        digest.update(block)
                        archive.write(block)
                output.flush()
                os.fsync(output.fileno())
            with gzip.open(temporary, 'rb') as stream:
                if checksum(stream) != digest.hexdigest():
                    raise ValueError('Compressed evidence did not reproduce the original bytes')
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError('Evidence changed during compression: ' + str(path))
            with temporary.open('rb') as stream:
                compressed_hash = checksum(stream)
            os.replace(temporary, destination)
            temporary = None
            row = dict(original_path=str(path.relative_to(directory)), compressed_path=str(destination.relative_to(directory)),
                       original_sha256=digest.hexdigest(), compressed_sha256=compressed_hash,
                       original_bytes=before.st_size, compressed_bytes=destination.stat().st_size)
            report['files'].append(row)
            # Save the verification record before removing the redundant plain copy.
            staging_report = report_path.with_suffix('.json.tmp')
            staging_report.write_text(json.dumps(report, indent=2) + '\n')
            os.replace(staging_report, report_path)
            path.unlink()
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    args = parser.parse_args()
    # Share the runner's lock so a collection cannot replace files during compression.
    with (args.run_directory.resolve().parent / '.experiment.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = compress(args.run_directory)
    print('Verified compressed evidence:', args.run_directory / 'compressed-evidence.json')
