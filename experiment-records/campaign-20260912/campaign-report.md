# Nautilus preliminary campaign — 12 September 2026

**Status: experiment collection complete; check destination and restoration status below.**

This campaign provides preliminary evidence for the simple research questions on partition-level measurement and consumer scaling. It tests scheduled actions with synthetic processing work on shared Nautilus nodes. It does not evaluate the complete adaptive selector, targeted reassignment, key splitting, a tuned HPA, or all rows and repetitions of the planned main study.

The retained sequence ledgers currently list 16 completed controlled trials out of 16 trials started. Prepared but unstarted conditions are listed in the committed protocols and are not counted as completed experiments.

## Measurement and comparison rules

- Each run uses a fresh topic, frozen shared configuration, 60-second warm-up and 120-second bounded drain in the controlled comparisons. This campaign does not delete its topics. Broker records remain subject to Kafka retention; durable outcome logs on the producer/consumer PVCs and their reconciled local copies are the retained message evidence.
- Latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, after application work and before commit acknowledgment. Unfinished messages have no observed completion latency and remain separately reported.
- Rolling 30-second Prometheus rates and histogram quantiles are monitoring views. Final cohort percentiles come from event evidence, not averages of consumer means or rolling p99 values.
- Actual application scraping is every 5 seconds. Historical exports use a 2-second query grid; this does not create independent new scrapes. Lag age combines exporter monotonic observation age and the age of the original Prometheus sample.
- Backlog integration uses only valid contiguous same-owner observations. Missing data and transition gaps are not zero. Backlog aggregates require at least 90% coverage in both runs; common-horizon CPU-request aggregates require at least 95%. Valid message outcomes are retained when monitoring coverage is low.
- Requested consumer CPU is integrated over the same evaluation-through-drain horizon in both conditions. It is not measured whole-cluster CPU, monetary cost, or resource use during preparation.
- The 99 ms threshold remains provisional; recorded clock probes do not establish a sufficiently tight independent cross-node bound for that SLA. No deadline-based success claim is made.
- Each family has matched workload seeds and opposite action orders across its two pairs. Initial assignments and node placements are recorded, not forced identical. Each metric reports its own eligible pair count. Two pairs provide descriptive repetition, not a strong confidence interval or a general causal/superiority claim.

## Controlled run inventory

| Family / pair | Action | Run | Evaluation admissions | Unfinished | Conditional completion p99 (s) | Lag coverage |
|---|---|---|---:|---:|---:|---:|
| sustained / L1 | scale | [run-20260912-064358](../run-20260912-064358/validation-report.md) | 810,000 | 0 | 95.580 | 93.70% |
| sustained / L1 | none | [run-20260912-070155](../run-20260912-070155/validation-report.md) | 810,000 | 92,148 | 292.424 | 99.63% |
| short / S1 | none | [run-20260912-071918](../run-20260912-071918/validation-report.md) | 180,000 | 2,241 | 121.822 | 98.33% |
| short / S1 | scale | [run-20260912-072753](../run-20260912-072753/validation-report.md) | 180,000 | 0 | 75.918 | 65.00% |
| sustained / L2 | none | [run-20260912-073652](../run-20260912-073652/validation-report.md) | 810,000 | 77,946 | 266.316 | 99.63% |
| sustained / L2 | scale | [run-20260912-075420](../run-20260912-075420/validation-report.md) | 810,000 | 0 | 79.096 | 95.93% |
| short / S2 | scale | [run-20260912-081218](../run-20260912-081218/validation-report.md) | 180,000 | 0 | 62.936 | 80.00% |
| short / S2 | none | [run-20260912-082113](../run-20260912-082113/validation-report.md) | 180,000 | 0 | 115.817 | 98.33% |
| isolated / D1 | none | [run-20260912-085711](../run-20260912-085711/validation-report.md) | 360,000 | 185,039 | 259.655 | 99.17% |
| isolated / D1 | scale | [run-20260912-090802](../run-20260912-090802/validation-report.md) | 359,999 | 159,847 | 246.733 | 89.17% |
| isolated / D2 | scale | [run-20260912-091909](../run-20260912-091909/validation-report.md) | 360,000 | 164,572 | 250.328 | 94.17% |
| isolated / D2 | none | [run-20260912-093023](../run-20260912-093023/validation-report.md) | 360,000 | 191,127 | 268.532 | 99.17% |
| low-input / A1 | none | [run-20260912-094344](../run-20260912-094344/validation-report.md) | 72,000 | 0 | 0.824 | 98.33% |
| low-input / A1 | scale | [run-20260912-095159](../run-20260912-095159/validation-report.md) | 72,000 | 0 | 0.939 | 91.67% |
| low-input / A2 | scale | [run-20260912-100035](../run-20260912-100035/validation-report.md) | 72,000 | 0 | 0.900 | 93.33% |
| low-input / A2 | none | [run-20260912-100910](../run-20260912-100910/validation-report.md) | 72,000 | 0 | 0.825 | 98.33% |

## Per-family paired results

### sustained

Status: complete_predeclared_family. [Complete tables, timing and limits](comparisons/sustained/comparison-report.md).

Pair L1 (seed 21): unfinished 11.376% → 0.000%; completed-cohort p99 292.424 → 95.580 s; requested consumer CPU 66.000 → 125.749 core-minutes. The arrow compares keep-3 with schedule-3-to-6.

Pair L2 (seed 22): unfinished 9.623% → 0.000%; completed-cohort p99 266.316 → 79.096 s; requested consumer CPU 66.000 → 125.639 core-minutes. The arrow compares keep-3 with schedule-3-to-6.

![sustained paired outcomes](comparisons/sustained/paired-outcomes.png)

### short

Status: complete_predeclared_family. [Complete tables, timing and limits](comparisons/short/comparison-report.md).

Pair S1 (seed 31): unfinished 1.245% → 0.000%; completed-cohort p99 121.822 → 75.918 s; requested consumer CPU 24.000 → 41.773 core-minutes. The arrow compares keep-3 with schedule-3-to-6.
scale limitations: Lag coverage below 90%; transition gaps remain part of the result.

Pair S2 (seed 32): unfinished 0.000% → 0.000%; completed-cohort p99 115.817 → 62.936 s; requested consumer CPU 24.000 → 41.781 core-minutes. The arrow compares keep-3 with schedule-3-to-6.
scale limitations: Lag coverage below 90%; transition gaps remain part of the result.

![short paired outcomes](comparisons/short/paired-outcomes.png)

### isolated

Status: complete_predeclared_family. [Complete tables, timing and limits](comparisons/isolated/comparison-report.md).

Both no-action runs placed partition 0 on consumer-sts-1 / k8s-gpu-01, whereas both scale runs initially placed it on consumer-sts-0 / patternlab. The scale trials later transferred it to consumer-sts-5 / k8s-ravi-01 in D1 and consumer-sts-3 / k8s-chase-ci-10 in D2. Curves already differ before the scheduled request. Opposite action order did not remove this initial-placement confounding; the differences below are observations under those conditions, not an isolated causal effect of replica count. The hot partition continued to accumulate backlog after scaling in the valid observed intervals.

Pair D1 (seed 41): unfinished 51.400% → 44.402%; completed-cohort p99 259.655 → 246.733 s; requested consumer CPU 36.000 → 65.779 core-minutes. The arrow compares keep-3 with schedule-3-to-6.
scale limitations: Lag coverage below 90%; transition gaps remain part of the result.

Pair D2 (seed 42): unfinished 53.091% → 45.714%; completed-cohort p99 268.532 → 250.328 s; requested consumer CPU 36.000 → 65.776 core-minutes. The arrow compares keep-3 with schedule-3-to-6.

![isolated paired outcomes](comparisons/isolated/paired-outcomes.png)

### low-input

Status: complete_predeclared_family. [Complete tables, timing and limits](comparisons/low-input/comparison-report.md).

The saved backlog-threshold confirmation is a health check when backlog was already below 1,500 offsets before the action. It must not be presented as recovery from prior overload. The complete pair report retains valid pre-action and full-evaluation backlog ranges, coverage and each run outcome.

Pair A1 (seed 51): unfinished 0.000% → 0.000%; completed-cohort p99 0.824 → 0.939 s; requested consumer CPU 24.000 → 41.776 core-minutes. The arrow compares keep-3 with schedule-3-to-6.

Pair A2 (seed 52): unfinished 0.000% → 0.000%; completed-cohort p99 0.825 → 0.900 s; requested consumer CPU 24.000 → 41.729 core-minutes. The arrow compares keep-3 with schedule-3-to-6.

![low-input paired outcomes](comparisons/low-input/paired-outcomes.png)

## Calibration and retained technical attempts

| Run | Role in the campaign |
|---|---|
| [run-20260912-031020](../run-20260912-031020/validation-report.md) | Six-consumer, 12,000/s, no-added-work calibration; no intended sustained backlog. Existing HPA active, six replicas observed. |
| [run-20260912-042303](../run-20260912-042303/validation-report.md) | Excluded: HPA restored six after three were requested; interrupted trial and cleanup diagnostic. |
| [run-20260912-043547](../run-20260912-043547/validation-report.md) | Three-consumer no-added-work calibration; message outcomes retained, resource comparison excluded after Mac sleep caused poor coverage. Historical Prometheus export recovered with original failure status retained. |
| [run-20260912-052133](../run-20260912-052133/validation-report.md) | Excluded: group-generation commit errors prevented readiness; no production released. |
| [run-20260912-053411](../run-20260912-053411/validation-report.md) | Limited pressure diagnostic; message outcomes retained, mixed-field monitoring coverage below the qualifying threshold. |
| [run-20260912-054927](../run-20260912-054927/validation-report.md) | Qualified pressure calibration: 180,000 evaluation admissions, 3,103 unfinished, fixed ownership and 98.33% lag coverage. |
| [run-20260912-083417](../run-20260912-083417/validation-report.md) | Qualified dedicated-partition calibration: one partition and consumer, 1,200 admissions/s, 639.183 unique completions per evaluation second, 24,687 of 144,000 admissions unfinished by drain, fixed ownership and 98.33% lag coverage. |
| [run-20260912-060649](../run-20260912-060649/validation-report.md) | Excluded interrupted scale attempt: the legacy freshness guard compared wall clocks across machines. |
| [run-20260912-062803](../run-20260912-062803/validation-report.md) | Excluded interrupted attempt: initial PromQL union omitted required original-sample timestamps; stopped before the scale action. |
| [run-20260912-063614](../run-20260912-063614/validation-report.md) | Excluded pre-production diagnostic: an overly strict readiness check rejected unresolved positions on an empty topic. No production released. |

[Balanced-input calibration figure and exact consumer counts](calibration-diagnostic/calibration-diagnostic.json) show that balanced arrivals can coexist with uneven processing progress. They do not isolate hardware differences, contention or another cause on shared nodes.

[Dedicated-partition qualification](single-partition-calibration/qualification.json) records all predeclared checks. Median backlog in the three boundary windows grew from 37,580 to 65,910 to 98,837 offsets. This establishes overload for that consumer, node and interval, not a universal service-capacity constant. Later concentrated-input runs may have different owners, placements and additional cold-partition traffic.

## Relationship to the proposal experiment table

| Table 1 condition | What this campaign can support |
|---|---|
| A — balanced demand with sufficient capacity | Separate lower-input action comparisons at 600/s; completed trials appear above. Claim headroom only when their measured no-action outcomes confirm it. |
| B — balanced sustained pressure | Controlled CPU-workload no-action/scaling pairs with common follow-up and separate resource accounting. |
| C — busy partitions share a consumer with spare capacity elsewhere | Targeted reassignment is not implemented or tested here. |
| D — one overloaded sequential partition | A dedicated one-partition/one-consumer calibration qualified before the concentrated-input comparison. Both concentrated pairs are complete; initial hot-owner/node differences confound attributing their differences solely to consumer count. Cold traffic is retained separately. |
| E — short burst returning to ordinary demand | The short core workload ends production at zero input; it is an episode-duration pilot, not the full return-to-normal burst condition. |
| F — moving hotspots | Not tested in this campaign. |
| G — equal arrivals with differing record costs | All controlled records use the same synthetic CPU task. Uneven node progress is not the planned variable-record-cost experiment. |

## Evidence locations and reproducibility

Full local event evidence is under `results/RUN_ID/` in the active repository; original closed-run event files are retained on the Nautilus producer and consumer PVCs. `evidence-verification.json` compares local bytes with PVC SHA-256 values. Completed validated evidence may be stored as lossless gzip with a round-trip hash record. Git contains smaller configurations, outcome and monitoring summaries, plots, source identities and file manifests; it is not the full raw-data backup.

The paired source signatures compare the producer/consumer application files, Python and package versions that actually ran. Baseline run-20260912-070155 launched with HPA wrapper/docs/tests changes in the working tree, later committed as `6644c86`; the launch status is retained and the later commit is not claimed as its launch revision. Its direct scheduled path and application signatures match the paired run.

Local backups include a verified full Git bundle through `b42bbcf` and an incremental bundle through `f2e36f3`. Final commit, push, restoration and online proposal status are recorded separately at campaign completion. Do not treat a local commit as a successful GitHub push or a local LaTeX compile as an online Overleaf update.

## What the next study must resolve

The pressure pairs show why completion latency must be read with unfinished messages and requested resources. The concentrated pairs show a continuing hot-partition bottleneck after adding consumers, while their initial placement differences prevent attributing the size of the change to replica count alone. The lower-input controls test whether intervention is needed when the measured baseline already keeps up. These observations motivate the research questions; they do not establish that existing papers ignored the problem.

The next controlled comparison should constrain or deliberately cross hot-partition owner/node placement, retain pre-action curves, and separate tuning trials from evaluation trials. A supported targeted-reassignment path needs ownership/progress and application-correctness checks before it is compared with scaling. The full main study still needs the Table 1 burst-to-normal, moving-hotspot and variable-record-cost conditions, stronger policy baselines, and the planned independent repetitions. Increasing the number of messages in one run does not supply those repetitions.

Figures in the proposal use the first predeclared pair in the applicable family, rather than selecting the most favorable repetition. Every pair and technical exclusion remains available in this record.

## Final operational and destination status

See [campaign completion status](campaign-completion.json) for restored cluster settings, source validation, proposal layout verification and publication status. [Archived orchestration sources](orchestration/README.md) preserve the dated helpers and their exact protocol/report hashes outside the active runtime. Local code lives in the final Desktop code folder. Full Git-bundle backup manifests and private progress correspondence remain in the local campaign audit folder, outside this public record.
