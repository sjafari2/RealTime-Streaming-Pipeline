# Third traffic-concentration repetition

Status: all eight additional trials completed and validated.

This batch adds one repetition of each of eight combinations at 600 total messages/s. Four or twelve of sixty partitions receive 80% of input, initially owned by Consumer 0. Responses are Keep 3, redistribution within 3, scaling to 6 with targeted redistribution, and normal Kafka scaling to 6. Repetition 3 uses workload seed 83.

The original and added-consumer placements are fixed to the matched November 4 campaign. A fresh balanced reference and both Keep 3 pressure controls must pass before mitigation trials. Each comparison retains 60 seconds of warm-up, 600 seconds of evaluation and 120 seconds of drain, with the action requested at evaluation second 60.

Transient infrastructure failures may be retried only after verified restoration. Failed attempts remain in the evidence, while valid adverse outcomes remain accepted experimental outcomes. Scientific admission failures stop the batch.

Raw evidence is stored in `/Users/soheila/Library/Application Support/StreamProcessing/matched-skew600-repeat3-20261110`, with verified local copies under `/Users/soheila/Desktop/Thesis-26-27/backup/matched-skew600-repeat3-20261110`. These copies do not constitute independent off-device backup.

Plots have been saved and visually reviewed without insertion into the proposal. Earlier runs were combined with this repetition after comparability checks. The separately authorized starting-backlog repetition follows this traffic-concentration batch.

Validation before launch: 11 targeted tests passed, including additional-repetition ordering, pressure-gate preservation on resume, placement checks, balanced admission and restoration-aware retry eligibility.

## Completed third repetition

All eight additional trials passed validation and restoration was verified on 11 November 2026. The comparison now contains three independent runs for each of eight concentration/response conditions. The combined analysis checked the matching protocol, runtime configuration, placement, software and evidence hashes before combining the prior sixteen and new eight trials. `completion.json` lists accepted run identities and retained raw evidence. Reviewed technical failures remain recorded and are excluded from performance summaries.

`run-statistics.json` retains individual outcomes and equal-weight means with sample standard deviations. Unconfirmed recovery is not assigned zero and no unconditional recovery mean is reported for incomplete groups. Observed CPU and RSS integrals retain their measured coverage. Capacity remains unmeasured where matching busy calibration is unavailable. The five PNG/PDF/SVG figure sets have been saved and visually reviewed. Proposal insertion remains pending user approval.
