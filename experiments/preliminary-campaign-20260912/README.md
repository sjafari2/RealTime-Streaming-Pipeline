# Bounded preliminary campaign, 12 September 2026

This campaign addresses simple research questions 1 and 2: valid partition-level
measurements and the outcome/cost of scheduled consumer scale-out. The work budget
starts at 08:53 UTC and ends at 20:53 UTC. This is a short preliminary campaign,
not the proposal's full Table 1, 25-minute, five-repetition study.

First calibrate real backlog with the committed 180-second balanced pressure
configuration (12,000 target messages/s total, no artificial application work).
Inspect the trial before selecting another. If rate alone does not create an
informative consumer limit, explicitly calibrate the existing SHA-256 task at a
lower message rate. Keep the task and generated workload fixed within comparisons.

Then use matched no-action and scheduled scale-out trials. Freeze timing, workload,
replicas and seed before each pair; preserve both results regardless of outcome.
Repeat valid pairs if time and storage allow. Add a concentrated-partition case only
when its measured input exceeds the owner's processing capacity. Ordinary skew
alone does not establish an indivisible-partition limit. Reassignment, splitting
and a new automatic controller are outside this campaign.

Save admitted/completed rates, partition ownership, completion backlog, completion
latency plus unfinished records, monitoring coverage, action/readiness/first-useful
completion events, placement and resource-time. Preserve per-message evidence on
Nautilus and locally; keep only small summaries/configurations/checksums/plots in
Git. Current producer PVC and local free space constrain trial size; check them
before each trial. Use the calibration plan's conservative storage and stop limits.
Reserve time to inspect plots and reconcile the preliminary-results proposal text.

The observed 99 ms threshold is provisional. Point-in-time kernel clock reports
are not an independently established cross-node error bound. These pilots document
research progress and limitations; they cannot establish legal eligibility,
first-ever novelty, or superiority over all prior systems.

## Monitoring preparation

The live broker JMX endpoint took 11.79 seconds for an idle request, exceeding its
10-second scrape interval. The revised job discovers each broker separately and
uses a 30-second interval with a 25-second timeout. Application pod discovery also
includes consumers added during scaling. The deployed Prometheus 2.52 promtool
accepted the proposed configuration and its existing alert rules. Deployment and
post-deployment health are recorded in the campaign audit before workload begins.
No broker restart or data deletion is required.

Configuration semantics: https://prometheus.io/docs/prometheus/latest/configuration/configuration/
