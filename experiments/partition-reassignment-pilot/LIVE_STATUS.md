# Redistribution pilot: execution status

No new trial has run. Nautilus OIDC browser authentication timed out before the
pod inventory could be read. No cluster source or configuration was changed.
The eight performance trials remain authorized, subject to the validation and
calibration gates in README.md.

## Locally implemented

- `src/common/explicit_assignment.py`: fixed-membership startup map, completed-prefix
  commit/readback, durable evidence flush, release, acquire and resume checks.
- `src/consumer/consumer.py`: opt-in explicit assignment at processing batch boundaries.
  The default cooperative group mode remains available.
- `my-shell/explicit_control.py`: require all consumers to release before any acquire,
  verify offsets and process identities, then resume. Failure aborts the managed run.
- `my-shell/run_experiment.py`: scheduled `redistribute` action with a complete JSON
  target list passed through `--target-assignment`. Frozen configuration must set
  `CONSUMER_ASSIGNMENT_MODE=explicit`, `CONSUMER_STATIC_MEMBERSHIP=false`, and
  `EXPLICIT_ASSIGNMENT_JSON` to the complete initial ownership list.
- `src/common/pipeline_runtime.py`: durable evidence barrier for handoffs.
- `my-shell/sync_code.py`: include the new consumer dependency in shared-source sync.

Both arms must use explicit assignment and synchronous periodic commits. The
transfer pauses **all consumers**, including consumers whose ownership is unchanged.
Any measured intervention cost therefore includes this global pause. This is a
synthetic-workload pilot, not a fault-tolerant group assignor. The scheduled target
is predeclared; do not describe it as a live adaptive decision. A target equal to
current ownership records a no-op without pausing consumers.

## Still required before performance trials

1. Restore Nautilus authentication and confirm the cluster is idle.
2. Back up shared sources and settings, sync the committed source, and verify hashes.
3. Run a small nonempty-topic handoff test. Reconcile acknowledged message IDs,
   completion evidence, offset continuity and ownership; inspect prefetched work,
   swaps and interruption behavior. Local mocks do not establish broker correctness.
4. Calibrate actual capacities and verify imbalanced backlog growth and destination
   headroom. The 400 messages/s capacities in review-plan.json remain illustrative.
5. Freeze the calibrated settings and validated run block; execute the eight
   comparisons, collect evidence/plots, then restore original shared settings.

Implementation checks follow the Confluent Python client API for synchronous
commit results, committed offsets and explicit assignment:
https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

## Local validation

207 tests passed in the final code folder with:

```bash
PYTHONDONTWRITEBYTECODE=1 /tmp/pipeline-review-venv/bin/python -m pytest -q -p no:cacheprovider --ignore-glob='* 2.py'
```

The initial unrestricted collection also discovered existing untracked ` 2.py`
copies and failed on duplicate Prometheus registrations. Those unrelated copies
were preserved and excluded from the intended-suite run. `git diff --check` passed.
No live Kafka correctness or performance claim follows from these local checks.
