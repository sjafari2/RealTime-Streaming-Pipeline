# Performance and signals by reporting interval

Before: seconds 30–60; Active–end: verified activation to 600 (60–600 for Keep 3); Eval. end: seconds 570–600. Throughput counts distinct completions in the interval, including warm-up messages completed then. Interval p99 uses evaluation messages completed there, retaining their full creation-to-completion latency. All intervals exclude drain.

Mean backlog and mean skew use equally weighted valid original observations. Pending counts refer to evaluation messages at each interval endpoint, not unfinished messages after drain.

## Smaller starting backlog, Run 1

| Response | Interval | Throughput (msg/s) | p99 (s) | Mean backlog (offsets) | Mean skew | Pending at endpoint | Valid/expected signal samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Keep 3 | Before | 360.10 | 28.713 | 9,990.4 | 5.14 | 14,472 | 15/15 |
| Keep 3 | Active–end | 362.47 | 282.647 | 78,369.3 | 4.91 | 142,738 | 270/270 |
| Keep 3 | Eval. end | 370.80 | 285.859 | 138,447.3 | 4.88 | 142,738 | 15/15 |
| Redistribute within 3 | Before | 357.73 | 28.744 | 10,421.9 | 5.15 | 14,515 | 15/15 |
| Redistribute within 3 | Active–end | 648.37 | 52.497 | 2,026.4 | 10.55 | 52 | 251/251 |
| Redistribute within 3 | Eval. end | 600.30 | 0.748 | 49.7 | 11.71 | 52 | 15/15 |
| Scale + targeted redistribution | Before | 358.20 | 28.728 | 10,235.2 | 5.16 | 14,465 | 15/15 |
| Scale + targeted redistribution | Active–end | 660.60 | 59.206 | 643.7 | 11.65 | 56 | 221/223 |
| Scale + targeted redistribution | Eval. end | 600.00 | 0.235 | 27.1 | 10.33 | 56 | 15/15 |
| Scale + Kafka rebalance | Before | 358.07 | 28.913 | 10,103.7 | 5.15 | 14,552 | 15/15 |
| Scale + Kafka rebalance | Active–end | 622.42 | 55.697 | 12,531.8 | 6.43 | 7,716 | 250/250 |
| Scale + Kafka rebalance | Eval. end | 618.77 | 20.879 | 8,042.4 | 6.25 | 7,716 | 15/15 |

## Smaller starting backlog, Run 2

| Response | Interval | Throughput (msg/s) | p99 (s) | Mean backlog (offsets) | Mean skew | Pending at endpoint | Valid/expected signal samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Keep 3 | Before | 355.53 | 28.993 | 10,121.1 | 5.15 | 14,602 | 15/15 |
| Keep 3 | Active–end | 358.59 | 287.001 | 78,814.8 | 4.89 | 144,963 | 269/270 |
| Keep 3 | Eval. end | 372.83 | 290.212 | 140,771.2 | 4.85 | 144,963 | 15/15 |
| Redistribute within 3 | Before | 356.67 | 29.147 | 10,412.0 | 5.14 | 14,673 | 15/15 |
| Redistribute within 3 | Active–end | 650.92 | 53.339 | 2,107.9 | 10.64 | 39 | 249/249 |
| Redistribute within 3 | Eval. end | 600.40 | 0.167 | 44.6 | 9.01 | 39 | 15/15 |
| Scale + targeted redistribution | Before | 358.63 | 28.541 | 9,931.9 | 5.16 | 14,400 | 15/15 |
| Scale + targeted redistribution | Active–end | 673.68 | 72.918 | 1,326.2 | 11.23 | 68 | 218/218 |
| Scale + targeted redistribution | Eval. end | 599.90 | 0.315 | 31.8 | 11.01 | 68 | 15/15 |
| Scale + Kafka rebalance | Before | 355.93 | 29.235 | 10,716.0 | 5.14 | 14,730 | 14/15 |
| Scale + Kafka rebalance | Active–end | 624.00 | 25.470 | 433.0 | 10.34 | 79 | 250/253 |
| Scale + Kafka rebalance | Eval. end | 599.87 | 0.219 | 53.6 | 9.98 | 79 | 13/15 |

## Larger starting backlog, Run 1

| Response | Interval | Throughput (msg/s) | p99 (s) | Mean backlog (offsets) | Mean skew | Pending at endpoint | Valid/expected signal samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Keep 3 | Before | 363.33 | 0.217 | 39,564.5 | 4.94 | 29,943 | 15/15 |
| Keep 3 | Active–end | 364.81 | 339.088 | 107,261.7 | 4.89 | 170,837 | 269/270 |
| Keep 3 | Eval. end | 367.73 | 341.928 | 166,606.7 | 4.86 | 170,837 | 15/15 |
| Redistribute within 3 | Before | 359.67 | 0.349 | 39,030.3 | 4.95 | 29,947 | 15/15 |
| Redistribute within 3 | Active–end | 706.10 | 110.777 | 8,636.1 | 11.55 | 59 | 251/251 |
| Redistribute within 3 | Eval. end | 599.83 | 0.170 | 41.5 | 8.39 | 59 | 15/15 |
| Scale + targeted redistribution | Before | 357.53 | 0.156 | 39,805.3 | 4.94 | 29,946 | 15/15 |
| Scale + targeted redistribution | Active–end | 779.62 | 159.091 | 5,065.6 | 11.25 | 50 | 198/198 |
| Scale + targeted redistribution | Eval. end | 600.03 | 0.191 | 25.6 | 11.44 | 50 | 15/15 |
| Scale + Kafka rebalance | Before | 356.00 | 0.292 | 39,813.0 | 4.95 | 29,943 | 15/15 |
| Scale + Kafka rebalance | Active–end | 675.27 | 86.534 | 2,064.1 | 9.66 | 71 | 249/249 |
| Scale + Kafka rebalance | Eval. end | 600.07 | 0.202 | 44.5 | 8.52 | 71 | 15/15 |

## Larger starting backlog, Run 2

| Response | Interval | Throughput (msg/s) | p99 (s) | Mean backlog (offsets) | Mean skew | Pending at endpoint | Valid/expected signal samples |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Keep 3 | Before | 359.73 | 0.423 | 39,855.3 | 4.97 | 30,049 | 15/15 |
| Keep 3 | Active–end | 361.54 | 343.356 | 108,212.9 | 4.87 | 172,983 | 269/270 |
| Keep 3 | Eval. end | 368.77 | 346.367 | 168,638.9 | 4.84 | 172,983 | 15/15 |
| Redistribute within 3 | Before | 359.80 | 0.181 | 40,051.3 | 4.97 | 30,047 | 15/15 |
| Redistribute within 3 | Active–end | 708.47 | 106.016 | 8,737.6 | 11.50 | 43 | 250/250 |
| Redistribute within 3 | Eval. end | 600.23 | 1.116 | 52.7 | 10.44 | 43 | 15/15 |
| Scale + targeted redistribution | Before | 351.57 | 0.302 | 40,367.6 | 4.96 | 30,049 | 15/15 |
| Scale + targeted redistribution | Active–end | 738.60 | 133.258 | 3,911.4 | 11.83 | 60 | 221/222 |
| Scale + targeted redistribution | Eval. end | 600.23 | 0.253 | 34.7 | 11.31 | 60 | 15/15 |
| Scale + Kafka rebalance | Before | 359.73 | 0.159 | 39,388.3 | 4.97 | 30,046 | 15/15 |
| Scale + Kafka rebalance | Active–end | 675.19 | 92.376 | 2,354.9 | 9.47 | 68 | 248/249 |
| Scale + Kafka rebalance | Eval. end | 601.10 | 0.396 | 40.5 | 9.79 | 68 | 15/15 |

## Assignment after scaling

These counts describe high-traffic partitions specified by the workload, not partitions classified as hot from measured lag. Each native Kafka scaling run ended with ten total partitions per consumer.

| Starting backlog | Run | C0 | C1 | C2 | C3 | C4 | C5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Smaller | 1 | 2 | 0 | 0 | 0 | 0 | 10 |
| Larger | 1 | 2 | 0 | 0 | 3 | 4 | 3 |
| Larger | 2 | 2 | 0 | 0 | 3 | 4 | 3 |
| Smaller | 2 | 2 | 0 | 0 | 3 | 4 | 3 |

Starting ownership and the original consumers were matched. Native scaling produced different final assignments, and added-consumer nodes could differ. These differences prevent attributing recovery differences solely to starting backlog.

Within each backlog condition, all four responses use the same input and three-consumer assignment before the action at 60 seconds. Similar initial trajectories are therefore expected. Smaller and larger preparations differ in warm-up input and retained backlog; evaluation input is 600 messages/s for both.
