# Current project status

Updated 15 September 2026. Active code and documentation are on `main`.

## Completed evidence

There are **24 performance trials**: 12 comparisons, each comprising an independent keep-three trial and an independent scale-to-six trial. Six workload/starting-condition categories have Run 1 and Run 2. The first 16 trials recorded initial ownership and placement without requiring them to match; the later eight verified matching initial ownership and the placement of the original consumers.

Scaling helped under sustained balanced pressure and in both later 80/20 comparisons. The matched single-partition comparisons were mixed: one improved and one worsened. These preliminary results do not establish that adding consumers resolves every bottleneck or that a complete adaptive policy is effective. See the [experiment register](../EXPERIMENT_REGISTER.md) and [per-run table data](../experiment-records/paired-table-20260914/paired-results-data.json).

## Last recorded deployment check — 15 September 2026

Configuration and producer storage were replaced with tested UCSD shared volumes after central-storage mount failures. Original volumes were retained; historical contents were not migrated. Both application pods started, source synchronization verified 11 files, and 214 local tests passed under Python 3.12. Prometheus readiness passed; complete live metric coverage is still pending. Applications were scaled to zero after checks. See the [recovery record](../experiment-records/producer-storage-recovery-20260915/README.md).

## Next experiments — not yet run

Six balanced stability trials are planned: lower target rate with three consumers, pressure with three consumers, and pressure with scaling from three to six, each twice. Each lasts **23 minutes total: 1 minute warm-up, 20 minutes evaluation production, 2 minutes drain**. Scaling is scheduled five minutes after evaluation starts. Nominal aggregate targets are 600 and 1,500 messages/s; calibration must verify their intended operating conditions.

The [reviewed configurations](../experiments/stability-20260914/README.md) are prepared. The exact six-trial execution wrapper, sustained-window/aggregate reports, resource/rate calibration, complete monitoring checks and matching-start preparations remain pending. Passing local tests and recovering storage do not mean these trials have started.

Targeted reassignment has experimental code paths but no completed performance evaluation. Hot-key splitting, combined interventions and the full adaptive decision policy remain future work. The analysis reports whole-run completion statistics separately from rolling monitoring and includes unfinished work alongside latency.
