# Measurements around the scheduled responses

Before: evaluation seconds [30,60). Evaluation end: [570,600). Post-action: [verified activation,600), using explicit active_verified or a native six-owner map confirmed unchanged for ten seconds; Keep 3 uses second 60 as its no-action reference. Activation is not backlog recovery. No window includes drain. Rolling traces use trailing 30-second windows at 2-second steps, plotted at the window end.

Before and final-window means use valid sampled observations. The activation-to-end interval includes backlog recovery and excludes the preceding transition. Keep 3 uses second 60 as its reference. These intervals exclude drain.

## Completion performance

Throughput counts earliest distinct completions in [left,right), including warm-up records; p99 uses individual latencies of acknowledged evaluation-born messages whose earliest completion falls in that interval. Nearest rank; no averaging of percentiles. Completion precedes commit acknowledgment.

| Response | Run | Window | Throughput (msg/s) | Min (s) | Mean (s) | Max (s) | p99 (s) | Pending at end | Completed-window misses (%) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Keep 3 | 1 | Before | 342.467 | 0.011 | 4.362 | 59.627 | 59.566 | 30,686.000 | 7.207 |
| Keep 3 | 1 | Activation to end | 397.487 | 0.008 | 146.160 | 272.480 | 270.645 | 140,043.000 | 78.327 |
| Keep 3 | 1 | Final 30 s | 443.500 | 0.009 | 217.131 | 272.480 | 272.393 | 140,043.000 | 81.030 |
| Keep 3 | 2 | Before | 342.233 | 0.009 | 0.083 | 0.453 | 0.228 | 30,723.000 | 0.000 |
| Keep 3 | 2 | Activation to end | 347.304 | 0.010 | 146.125 | 325.124 | 321.129 | 167,208.000 | 75.355 |
| Keep 3 | 2 | Final 30 s | 345.700 | 0.013 | 239.217 | 325.124 | 324.890 | 167,208.000 | 75.335 |
| Redistribute 3 | 1 | Before | 346.600 | 0.010 | 30.047 | 49.993 | 49.854 | 25,786.000 | 66.454 |
| Redistribute 3 | 1 | Activation to end | 699.437 | 0.009 | 15.002 | 112.855 | 91.907 | 61.000 | 35.637 |
| Redistribute 3 | 1 | Final 30 s | 600.167 | 0.016 | 0.111 | 0.764 | 0.399 | 61.000 | 0.000 |
| Redistribute 3 | 2 | Before | 347.200 | 0.010 | 7.771 | 59.228 | 59.134 | 30,334.000 | 13.078 |
| Redistribute 3 | 2 | Activation to end | 684.609 | 0.009 | 11.664 | 119.696 | 82.676 | 54.000 | 30.112 |
| Redistribute 3 | 2 | Final 30 s | 600.400 | 0.027 | 0.115 | 0.722 | 0.449 | 54.000 | 0.000 |
| Targeted Scale 6 | 1 | Before | 341.200 | 0.010 | 0.087 | 0.846 | 0.352 | 30,887.000 | 0.000 |
| Targeted Scale 6 | 1 | Activation to end | 720.304 | 0.008 | 16.797 | 150.313 | 106.881 | 57.000 | 32.102 |
| Targeted Scale 6 | 1 | Final 30 s | 600.467 | 0.008 | 0.120 | 1.296 | 0.327 | 57.000 | 0.189 |
| Targeted Scale 6 | 2 | Before | 346.733 | 0.009 | 6.237 | 59.387 | 59.322 | 30,423.000 | 10.425 |
| Targeted Scale 6 | 2 | Activation to end | 709.269 | 0.007 | 14.077 | 146.989 | 103.485 | 61.000 | 28.075 |
| Targeted Scale 6 | 2 | Final 30 s | 600.367 | 0.009 | 0.112 | 0.548 | 0.222 | 61.000 | 0.000 |
| Kafka Scale 6 | 1 | Before | 344.867 | 0.010 | 2.391 | 59.811 | 59.777 | 30,784.000 | 3.868 |
| Kafka Scale 6 | 1 | Activation to end | 648.055 | 0.007 | 13.069 | 87.660 | 73.476 | 37.000 | 41.118 |
| Kafka Scale 6 | 1 | Final 30 s | 600.333 | 0.008 | 0.093 | 1.106 | 0.519 | 37.000 | 0.039 |
| Kafka Scale 6 | 2 | Before | 348.467 | 0.010 | 10.762 | 59.027 | 58.783 | 30,136.000 | 18.360 |
| Kafka Scale 6 | 2 | Activation to end | 625.350 | 0.007 | 35.059 | 100.664 | 91.254 | 16,745.000 | 64.555 |
| Kafka Scale 6 | 2 | Final 30 s | 607.833 | 0.008 | 28.052 | 44.776 | 44.733 | 16,745.000 | 63.384 |

## Backlog and workload signals

Arithmetic means of valid sampled values in the same half-open intervals as completion-window metrics. Process CPU is in cores, memory is process RSS. These are not requested resources or full-container memory.

B is fetched-offset lag; Q is processing backlog. Growth below is the mean of defined trailing ten-second Q-growth observations. All signal means are arithmetic; validity gaps remain missing.

| Response | Run | Window | Mean B (offsets) | Mean Q (offsets) | Mean skew | Mean Q growth (offsets/s) | Valid samples |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Keep 3 | 1 | Before | 25,912.267 | 25,912.267 | 14.099 | 262.747 | 15/15 |
| Keep 3 | 1 | Activation to end | 91,067.078 | 91,067.078 | 14.111 | 204.201 | 270/270 |
| Keep 3 | 1 | Final 30 s | 137,214.067 | 137,214.067 | 14.099 | 160.433 | 15/15 |
| Keep 3 | 2 | Before | 26,744.667 | 26,744.667 | 14.336 | 260.107 | 15/15 |
| Keep 3 | 2 | Activation to end | 99,319.772 | 99,319.772 | 14.097 | 252.521 | 268/270 |
| Keep 3 | 2 | Final 30 s | 163,238.000 | 163,238.000 | 14.046 | 249.633 | 15/15 |
| Redistribute 3 | 1 | Before | 21,244.267 | 21,244.267 | 14.247 | 249.480 | 15/15 |
| Redistribute 3 | 1 | Activation to end | 6,328.415 | 6,328.415 | 25.209 | -100.110 | 234/234 |
| Redistribute 3 | 1 | Final 30 s | 46.067 | 46.067 | 18.750 | -0.067 | 15/15 |
| Redistribute 3 | 2 | Before | 25,790.533 | 25,790.533 | 14.334 | 256.767 | 15/15 |
| Redistribute 3 | 2 | Activation to end | 4,781.602 | 4,781.602 | 23.377 | -84.325 | 251/251 |
| Redistribute 3 | 2 | Final 30 s | 52.667 | 52.667 | 19.790 | 0.253 | 15/15 |
| Targeted Scale 6 | 1 | Before | 26,987.200 | 26,987.200 | 14.155 | 251.013 | 15/15 |
| Targeted Scale 6 | 1 | Activation to end | 6,101.108 | 6,101.108 | 27.489 | -126.834 | 222/222 |
| Targeted Scale 6 | 1 | Final 30 s | 40.200 | 40.200 | 20.560 | -0.087 | 15/15 |
| Targeted Scale 6 | 2 | Before | 26,244.933 | 26,244.933 | 14.337 | 257.027 | 15/15 |
| Targeted Scale 6 | 2 | Activation to end | 4,929.995 | 4,929.995 | 27.921 | -123.704 | 222/224 |
| Targeted Scale 6 | 2 | Final 30 s | 38.333 | 38.333 | 22.390 | -0.060 | 15/15 |
| Kafka Scale 6 | 1 | Before | 26,676.667 | 26,676.667 | 14.159 | 251.707 | 15/15 |
| Kafka Scale 6 | 1 | Activation to end | 6,994.915 | 6,994.915 | 20.394 | -57.888 | 247/253 |
| Kafka Scale 6 | 1 | Final 30 s | 46.933 | 46.933 | 22.049 | -0.200 | 15/15 |
| Kafka Scale 6 | 2 | Before | 25,839.200 | 25,839.200 | 14.364 | 242.333 | 15/15 |
| Kafka Scale 6 | 2 | Activation to end | 20,612.964 | 20,612.964 | 19.947 | -32.467 | 251/252 |
| Kafka Scale 6 | 2 | Final 30 s | 16,782.667 | 16,782.667 | 19.446 | -9.973 | 15/15 |

## Actual process resources

CPU is measured in cores and memory is process RSS in MiB. Complete per-pod counts and coverage are in the JSON record.

| Response | Run | Window | Consumer CPU | Consumer RSS | Producer CPU | Producer RSS |
| --- | --- | --- | --- | --- | --- | --- |
| Keep 3 | 1 | Before | 1.385 | 143.246 | 0.200 | 93.832 |
| Keep 3 | 1 | Activation to end | 1.369 | 253.939 | 0.202 | 94.210 |
| Keep 3 | 1 | Final 30 s | 1.346 | 308.986 | 0.204 | 94.282 |
| Keep 3 | 2 | Before | 1.403 | 144.429 | 0.182 | 93.392 |
| Keep 3 | 2 | Activation to end | 1.394 | 257.016 | 0.185 | 93.717 |
| Keep 3 | 2 | Final 30 s | 1.359 | 308.683 | 0.198 | 94.116 |
| Redistribute 3 | 1 | Before | 1.404 | 134.739 | 0.191 | 93.763 |
| Redistribute 3 | 1 | Activation to end | 2.181 | 237.438 | 0.199 | 95.050 |
| Redistribute 3 | 1 | Final 30 s | 1.836 | 237.556 | 0.205 | 95.223 |
| Redistribute 3 | 2 | Before | 1.407 | 142.555 | 0.191 | 93.364 |
| Redistribute 3 | 2 | Activation to end | 2.129 | 224.546 | 0.188 | 94.328 |
| Redistribute 3 | 2 | Final 30 s | 1.892 | 224.723 | 0.178 | 94.617 |
| Targeted Scale 6 | 1 | Before | 1.398 | 144.853 | 0.199 | 94.049 |
| Targeted Scale 6 | 1 | Activation to end | 2.458 | 373.551 | 0.196 | 94.702 |
| Targeted Scale 6 | 1 | Final 30 s | 2.043 | 373.703 | 0.191 | 94.767 |
| Targeted Scale 6 | 2 | Before | 1.404 | 143.913 | 0.203 | 93.534 |
| Targeted Scale 6 | 2 | Activation to end | 2.249 | 363.570 | 0.194 | 94.479 |
| Targeted Scale 6 | 2 | Final 30 s | 1.943 | 363.698 | 0.195 | 94.578 |
| Kafka Scale 6 | 1 | Before | 1.399 | 143.186 | 0.187 | 93.513 |
| Kafka Scale 6 | 1 | Activation to end | 1.735 | 310.001 | 0.190 | 93.921 |
| Kafka Scale 6 | 1 | Final 30 s | 1.612 | 310.172 | 0.188 | 94.035 |
| Kafka Scale 6 | 2 | Before | 1.408 | 142.586 | 0.183 | 93.849 |
| Kafka Scale 6 | 2 | Activation to end | 1.885 | 341.079 | 0.186 | 93.990 |
| Kafka Scale 6 | 2 | Final 30 s | 1.867 | 341.290 | 0.191 | 94.055 |

## Requested consumer resources

These are time integrals over each stated interval. The long activation-to-end interval has a different duration from the two thirty-second windows. They are distinct from actual process usage.

| Response | Run | Window | CPU (core-min) | Memory (GiB-min) | Coverage fraction |
| --- | --- | --- | --- | --- | --- |
| Keep 3 | 1 | Before | 3.000 | 1.125 | 1.000 |
| Keep 3 | 1 | Activation to end | 54.000 | 20.250 | 1.000 |
| Keep 3 | 1 | Final 30 s | 3.000 | 1.125 | 1.000 |
| Keep 3 | 2 | Before | 3.000 | 1.125 | 1.000 |
| Keep 3 | 2 | Activation to end | 54.000 | 20.250 | 1.000 |
| Keep 3 | 2 | Final 30 s | 3.000 | 1.125 | 1.000 |
| Redistribute 3 | 1 | Before | 3.000 | 1.125 | 1.000 |
| Redistribute 3 | 1 | Activation to end | 46.855 | 17.571 | 1.000 |
| Redistribute 3 | 1 | Final 30 s | 3.000 | 1.125 | 1.000 |
| Redistribute 3 | 2 | Before | 3.000 | 1.125 | 1.000 |
| Redistribute 3 | 2 | Activation to end | 50.247 | 18.843 | 1.000 |
| Redistribute 3 | 2 | Final 30 s | 3.000 | 1.125 | 1.000 |
| Targeted Scale 6 | 1 | Before | 3.000 | 1.125 | 1.000 |
| Targeted Scale 6 | 1 | Activation to end | 88.442 | 33.166 | 1.000 |
| Targeted Scale 6 | 1 | Final 30 s | 6.000 | 2.250 | 1.000 |
| Targeted Scale 6 | 2 | Before | 3.000 | 1.125 | 1.000 |
| Targeted Scale 6 | 2 | Activation to end | 89.642 | 33.616 | 1.000 |
| Targeted Scale 6 | 2 | Final 30 s | 6.000 | 2.250 | 1.000 |
| Kafka Scale 6 | 1 | Before | 3.000 | 1.125 | 1.000 |
| Kafka Scale 6 | 1 | Activation to end | 100.800 | 37.800 | 1.000 |
| Kafka Scale 6 | 1 | Final 30 s | 6.000 | 2.250 | 1.000 |
| Kafka Scale 6 | 2 | Before | 3.000 | 1.125 | 1.000 |
| Kafka Scale 6 | 2 | Activation to end | 100.400 | 37.650 | 1.000 |
| Kafka Scale 6 | 2 | Final 30 s | 6.000 | 2.250 | 1.000 |

Only completed evaluation records contribute to window latency and its deadline-miss fraction; these are separate from full-cohort deadline outcomes. Pending records are reported alongside it. Post-action durations vary with activation time and exclude the preceding action transition; they do not measure total intervention cost. The interval includes previously queued messages and subsequent backlog recovery. Latency uses original unadjusted producer and consumer clocks, so small differences require caution. Later recovery must not be inferred from p99 alone.

An aggregate process observation requires fresh values for every expected pod. Missing values remain absent. Valid sample counts, timestamps and completeness relative to the export grid are retained; a missing entire export interval remains a separate coverage limitation. No zero fill or interpolation.
