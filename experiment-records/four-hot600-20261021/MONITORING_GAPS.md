# Monitoring gaps in the runtime-state figure

The four-partition, 600-message/s runtime-state figure is Figure 15 in the current proposal. Its gaps represent unavailable signal values, not zero backlog or evidence that Kafka lost messages.

- The initial window needs 15 valid observations for backlog, skew and persistence, and 15 intervals for growth. About the first 30 seconds is therefore window initialization.
- A snapshot is rejected when all partition owners are not observed or a partition offset measurement is invalid or stale. Validity requires fresh observations from every partition. Ownership changes also restart the rolling window.
- After a discontinuity, the rolling signals need a new contiguous window. A single absent two-second-grid observation can therefore produce approximately 30 seconds of missing rolling values. Raw partition-lag curves can resume earlier.

In Run 2, Keep 3 has invalid snapshots at seconds 146 and 488. Targeted Scale 6 has invalid/stale snapshots during seconds 136–146 around the handover, and isolated incomplete snapshots at seconds 310 and 430. At second 310, Consumer 0's ownership and completion-counter series are absent from the saved Prometheus export, but return at second 312. The corresponding pattern at second 430 affects Consumer 4, returning at second 432. Adjacent valid snapshots retain the same process identities and assignments at those isolated gaps.

Separate event logs record 580 completed attempts by Consumer 0 during seconds 308–312 and 70 by Consumer 4 during seconds 428–432. Processing therefore continued around these missing monitoring samples. The event-outcome audits for all four Run 2 trials report no validity failures; this does not reconstruct the missing lag values or establish behavior at every instant.

The saved export does not include Prometheus `up` or scrape-error diagnostics. It cannot distinguish a failed scrape, transient endpoint or network unavailability, or another collection issue as the cause of every isolated absence. No such root cause is asserted. Before future trials, retain those diagnostics and correlate them with consumer and broker logs.

Missing values remain absent. Interpolating across a handover could conceal its transient effects, and treating incomplete partition totals as zero would understate backlog. Rolling-signal coverage is lower than raw-snapshot coverage because of the window reset.

[Audit and source hashes](monitoring-gaps.json)
