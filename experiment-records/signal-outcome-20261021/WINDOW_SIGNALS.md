# Backlog and skew in the performance intervals

Arithmetic means of valid raw total lag B=sum(partition lag) and skew=max(partition lag)/mean(partition lag), not time-weighted means or means of rolling means.

Before uses nominal seconds 30,32,...,58; activation-to-end uses query samples at or after the unrounded verified activation and before second 600 (Keep 3 starts at 60); final uses 570,572,...,598. All exclude drain.

Valid samples / expected two-second query samples, including completely absent grid points in the denominator. No zero fill, interpolation or requirement for a new rolling window after a gap.

Exported timestamps have millisecond precision. Samples within 1 ms of the nominal two-second query grid are assigned to that grid before interval selection. This prevents an unintended one-sample shift in windows when export rounding precedes a boundary; completion-event intervals and original outcomes are unchanged.

Skew is a relative concentration measure: a large ratio can coexist with a very small total backlog. Read backlog and skew together. Post-activation means include recovery and may exceed final-window means. Rates and deployments are separate comparison blocks.

## 12hot-700

| Response | Run | Interval | Mean backlog (offsets) | Mean skew | Valid / expected samples |
| --- | --- | --- | ---: | ---: | ---: |
| Keep 3 | 1 | Before | 27,888.0 | 4.92 | 15/15 |
| Keep 3 | 1 | Activation to end | 107,288.7 | 4.89 | 270/270 |
| Keep 3 | 1 | Final 30 s | 177,266.3 | 4.91 | 15/15 |
| Keep 3 | 2 | Before | 28,233.1 | 4.95 | 15/15 |
| Keep 3 | 2 | Activation to end | 107,855.1 | 4.90 | 270/270 |
| Keep 3 | 2 | Final 30 s | 178,040.4 | 4.86 | 15/15 |
| Redistribute 3 | 1 | Before | 28,022.8 | 4.91 | 15/15 |
| Redistribute 3 | 1 | Activation to end | 5,224.8 | 10.62 | 253/253 |
| Redistribute 3 | 1 | Final 30 s | 47.9 | 8.60 | 15/15 |
| Redistribute 3 | 2 | Before | 29,396.3 | 4.94 | 15/15 |
| Redistribute 3 | 2 | Activation to end | 6,054.8 | 11.05 | 253/253 |
| Redistribute 3 | 2 | Final 30 s | 49.8 | 8.76 | 15/15 |
| Targeted scale 6 | 1 | Before | 28,167.7 | 4.93 | 15/15 |
| Targeted scale 6 | 1 | Activation to end | 2,815.3 | 10.05 | 230/230 |
| Targeted scale 6 | 1 | Final 30 s | 37.7 | 12.63 | 15/15 |
| Targeted scale 6 | 2 | Before | 29,522.9 | 4.94 | 15/15 |
| Targeted scale 6 | 2 | Activation to end | 3,045.9 | 10.45 | 227/227 |
| Targeted scale 6 | 2 | Final 30 s | 44.1 | 10.34 | 15/15 |
| Kafka scale 6 | 1 | Before | 28,166.5 | 4.93 | 15/15 |
| Kafka scale 6 | 1 | Activation to end | 3,104.1 | 11.22 | 251/251 |
| Kafka scale 6 | 1 | Final 30 s | 60.1 | 10.55 | 15/15 |
| Kafka scale 6 | 2 | Before | 29,172.1 | 4.95 | 15/15 |
| Kafka scale 6 | 2 | Activation to end | 1,512.9 | 9.58 | 249/249 |
| Kafka scale 6 | 2 | Final 30 s | 54.7 | 8.43 | 15/15 |

## 4hot-700

| Response | Run | Interval | Mean backlog (offsets) | Mean skew | Valid / expected samples |
| --- | --- | --- | ---: | ---: | ---: |
| Redistribute 3 | 1 | Before | 31,136.5 | 14.22 | 15/15 |
| Redistribute 3 | 1 | Activation to end | 4,062.3 | 21.48 | 252/252 |
| Redistribute 3 | 1 | Final 30 s | 46.5 | 20.31 | 15/15 |
| Redistribute 3 | 2 | Before | 31,738.9 | 14.06 | 15/15 |
| Redistribute 3 | 2 | Activation to end | 3,856.9 | 21.82 | 255/255 |
| Redistribute 3 | 2 | Final 30 s | 43.9 | 19.90 | 15/15 |
| Targeted scale 6 | 1 | Before | 31,301.9 | 14.20 | 15/15 |
| Targeted scale 6 | 1 | Activation to end | 6,317.9 | 25.37 | 227/227 |
| Targeted scale 6 | 1 | Final 30 s | 51.8 | 24.59 | 15/15 |
| Targeted scale 6 | 2 | Before | 31,503.2 | 14.04 | 15/15 |
| Targeted scale 6 | 2 | Activation to end | 6,325.8 | 25.01 | 225/225 |
| Targeted scale 6 | 2 | Final 30 s | 48.7 | 21.92 | 15/15 |

## 4hot-600

| Response | Run | Interval | Mean backlog (offsets) | Mean skew | Valid / expected samples |
| --- | --- | --- | ---: | ---: | ---: |
| Keep 3 | 1 | Before | 25,912.3 | 14.10 | 15/15 |
| Keep 3 | 1 | Activation to end | 91,067.1 | 14.11 | 270/270 |
| Keep 3 | 1 | Final 30 s | 137,214.1 | 14.10 | 15/15 |
| Keep 3 | 2 | Before | 26,217.6 | 14.33 | 15/15 |
| Keep 3 | 2 | Activation to end | 98,810.7 | 14.10 | 268/270 |
| Keep 3 | 2 | Final 30 s | 162,725.7 | 14.05 | 15/15 |
| Redistribute 3 | 1 | Before | 21,244.3 | 14.25 | 15/15 |
| Redistribute 3 | 1 | Activation to end | 6,328.4 | 25.21 | 234/234 |
| Redistribute 3 | 1 | Final 30 s | 46.1 | 18.75 | 15/15 |
| Redistribute 3 | 2 | Before | 25,790.5 | 14.33 | 15/15 |
| Redistribute 3 | 2 | Activation to end | 4,781.6 | 23.38 | 251/251 |
| Redistribute 3 | 2 | Final 30 s | 52.7 | 19.79 | 15/15 |
| Targeted scale 6 | 1 | Before | 26,486.9 | 14.15 | 15/15 |
| Targeted scale 6 | 1 | Activation to end | 6,128.5 | 27.50 | 221/221 |
| Targeted scale 6 | 1 | Final 30 s | 40.4 | 19.81 | 15/15 |
| Targeted scale 6 | 2 | Before | 26,244.9 | 14.34 | 15/15 |
| Targeted scale 6 | 2 | Activation to end | 4,930.0 | 27.92 | 222/224 |
| Targeted scale 6 | 2 | Final 30 s | 38.3 | 22.39 | 15/15 |
| Kafka scale 6 | 1 | Before | 26,176.1 | 14.15 | 15/15 |
| Kafka scale 6 | 1 | Activation to end | 7,023.2 | 20.39 | 246/252 |
| Kafka scale 6 | 1 | Final 30 s | 47.2 | 21.60 | 15/15 |
| Kafka scale 6 | 2 | Before | 25,348.3 | 14.36 | 15/15 |
| Kafka scale 6 | 2 | Activation to end | 20,628.7 | 19.95 | 250/251 |
| Kafka scale 6 | 2 | Final 30 s | 16,799.8 | 19.43 | 15/15 |
