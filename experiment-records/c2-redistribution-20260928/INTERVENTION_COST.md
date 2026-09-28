# Observed intervention costs

Treatment: Redistribute within 3. Cost criteria are specified in advance for this block. Cross-host timestamps have clock-probe uncertainty, so durations are approximate.

| Metric | Run 1 | Run 2 |
|---|---:|---:|
| No application-processing interval observed (s) | 15.3 | 15.3 |
| No completion interval (s) | 15.3 | 15.3 |
| Handoff coordination (s) | 19.3 | 19.4 |
| Net additional acknowledged-but-unfinished messages during coordination | 10,866.0 | 10,961.0 |
| Additional acknowledged-but-unfinished messages during processing gap | 10,700.0 | 10,682.0 |

Run 1: the primary recovery criterion was not confirmed before production ended. A short low-backlog interval alone is insufficient; see the unconfirmed-candidate fields in the JSON.

Run 2: the primary recovery criterion was not confirmed before production ended. A short low-backlog interval alone is insufficient; see the unconfirmed-candidate fields in the JSON.

**Processing interruption:** Gap between the last pre-resume completion and first post-resume processing start across all consumers; verified that no logged application processing interval intersects the gap. It is not pod downtime.

**Transition backlog:** Distinct messages with producer acknowledgment callback timestamp <= t and completion timestamp > t, including warm-up. Net change between release_requested and active_verified. This reconstructs acknowledged-but-unfinished work, not exact broker offset lag; callback delay and inter-host clock uncertainty affect boundary counts.

**Recovery:** First valid total processing-backlog observation <= 100 offsets that remains <= 100 for at least 30 seconds of consecutive observations, no gaps >3 seconds or owner/offset resets. Measured from resume_requested, during continuing production only. 50/200 thresholds supplied as sensitivity specified before this block.

**Interpretation:** Observed Redistribute within 3 overhead, not a causal estimate of excess backlog. Cross-host timestamps have the saved clock-probe uncertainty; report durations approximately.

The reconstructed message curve fills the monitoring gap using separate event evidence; it does not replace or interpolate missing lag observations. Recovery uses actual valid processing-backlog monitoring, not that reconstructed curve. Warm-up work is included in outstanding work. Counts are observed changes, not the number of messages that would have accumulated under a hypothetical no-action counterfactual.

[Machine-readable results and sensitivity](intervention-cost.json)

![Whole-run p99 and unfinished work](c2-redistribution-p99-unfinished.png)

![Reconstructed outstanding messages](intervention-cost-backlog.png)
