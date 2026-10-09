# Monitoring handover check

`verify_live.py` checks a collected technical redistribution run for correct assignment-start offsets, prompt return of valid lag observations, and passed message/offset validation. A passed record is required by the intervention campaign runner.

```bash
python3 experiments/monitoring-gap-20260929/verify_live.py \
  results/TECHNICAL_RUN results/TECHNICAL_RUN/monitoring-gap-verification.json
```

`TECHNICAL_RUN` is a placeholder for the nonempty handover check from the configured deployment. The script expects the original uncompressed consumer event files and collected manifest, monitoring, lag and handover-validation evidence. It stops with a failed record if its conditions are not met. An empty-topic readiness check alone cannot demonstrate this monitoring behavior.

The check is a technical prerequisite, not one of the 24 performance trials. Historical technical evidence remains on the archive branch; a passed historical record is not a measurement of current cluster health.
