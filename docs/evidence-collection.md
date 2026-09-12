# Collecting experiment evidence

The complete single-run and repeated-run commands both call `collect()` in
`my-shell/run_experiment.py` after the bounded drain and final process checks.
Collection downloads evidence; it does not start or repeat a workload.

Each role uses a shared PVC. The collector connects through one currently available
pod and lists every entry under that role's `evidence/RUN_ID` directory. It copies
each saved pod directory separately, including evidence from pods that were removed
after scaling. It does not limit collection to the pods currently running.

Smaller transfers avoid one long API stream for all consumers. Each copy retains
`kubectl cp --retries=3`, a 900-second overall command limit and no shorter API stream
deadline. An error from the API error stream, including `unexpected EOF`, causes a
fresh attempt for that directory, up to three attempts. A command timeout is reported
without starting another attempt. Ordinary Kubernetes requests retain their 30-second
API deadline.

Files first arrive in a temporary local directory. A failed directory retry removes
only its partial staging copy. The previous local role evidence is replaced only
after every entry for that role has copied successfully. Source evidence on Nautilus
is never removed by collection.

During `run-20260911-234758`, the initial combined consumer copy failed despite
kubectl's retries. Copying the six consumer directories separately recovered the
same completed run, and all 27 producer/consumer evidence files matched Nautilus by
SHA-256. The collector now uses this smaller-copy approach for both entry points.
This maintenance change was made after that workload; its original source revision
is preserved in the run record.

Transfer regression checks cover bounded retries, preservation of earlier local
evidence when a later copy fails, cleanup of partial downloads, inclusion of retired
pods and separation of stream/API timeouts. Exact outcome analysis and Prometheus
export remain separate checks; a successful transfer alone does not validate a run.
