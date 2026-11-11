# Action-code readiness correction

The traffic-concentration repetition was paused after the first redistribution attempt. The two Keep 3 controls remain accepted and their pressure checks passed.

For `run-20261111-030538`, the decision was recorded at evaluation second 69.810, the transfer routine began at approximately second 508.940, and activation was verified at second 539.209. The transfer-entry time follows from the routine's recorded deadline minus its fixed 60-second timeout. The Python cache for `explicit_control.py` was first created immediately before transfer entry. This supports a local runner preparation delay. The exact blocked filesystem call was not instrumented, so the record does not claim to identify that call.

The full attempt and a verified raw copy remain preserved. Its original accepted-trial record is retained in the campaign's `technical_attempts`, with the original campaign, series, and supervisor checkpoints saved under the evidence root's `reviewed-checkpoints`. The delayed attempt is excluded from the matched repetition based on its execution timing. Latency and recovery outcomes were not used to decide this exclusion. Report it as an additional technical attempt, rather than silently dropping it.

The runner now loads and verifies targeted-action modules before releasing a timed run and records readiness timestamps and source hashes. Consumer code, producer code, workload rates, partition maps, resources, node placement, phases, and monitoring thresholds are unchanged. This is a local coordinator correction and does not deploy new consumer or producer code.

The supervisor can continue after a documented infrastructure review while retaining its original attempts and logs. It still rejects scientific admission failures and unverified restoration. The interrupted 4-partition redistribution setup had not released traffic. The comparison resumes from the two passed controls and repeats the affected 12-partition redistribution condition.

## Preflight file-read recovery

After the third validated trial, the next preflight blocked while reading the local producer launch script. Sampling confirmed a file read in the coordinator. A fresh reader obtained the expected bytes after download, but the old reader remained blocked. The batch was stopped before the next workload began, and restoration was verified. All three accepted trials were retained.

Local source hashing now uses a short-lived reader with a 30-second timeout and at most three attempts. A timed-out reader is terminated and reaped before a fresh one is started. Failure still prevents source admission and traffic release. The existing supervisor may retry this infrastructure error only after verified restoration. Runtime source hashes must still match the deployed source. No workload settings, node placements, resource settings, consumer code or producer code changed.
