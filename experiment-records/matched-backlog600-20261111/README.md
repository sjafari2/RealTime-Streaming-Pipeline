# Starting-backlog comparison on the current fixed deployment

Eight main trials are authorized, one run for each smaller/larger preparation condition and each of Keep 3, redistribution within 3, scaling to 6 with targeted redistribution, and normal Kafka scaling. This is a separate deployment block from the October starting-backlog trials, whose client nodes differ. Do not pool them to create three-run means or standard deviations.

All runs use 60 partitions, with 12 high-traffic partitions receiving 80 percent of traffic and initially owned by Consumer 0. Aggregate warm-up rates are 300 and 900 messages/s for 60 seconds. Evaluation uses 600 messages/s for 600 seconds, followed by 120 seconds of drain. The scheduled action reference is evaluation second 60. Seed 91 is fixed across this new matched block.

Both no-intervention controls precede mitigation. They must pass the unchanged pressure gates. The larger preparation must have a larger measured backlog at the action reference. Logged evaluation partition traces, input rate, fixed nodes, resource settings and producer-consumer separation are checked before mitigation. A fresh balanced reference and technical readiness checks are recorded separately from the eight main trials. This preparation changes message age and backlog together and does not isolate their effects.

The runner retains failures, checks restoration before retrying infrastructure interruptions, and does not retry valid unfavorable outcomes to improve results. Source changes are frozen while the campaign runs. The final source remains in Desktop/Thesis-26-27/code. An identical committed execution checkout outside cloud synchronization may be used with the same result directory and command lock.

Validation before launch: 35 focused tests passed, including admission checks for the separate backlog analysis. The default traffic dry-run protocol is unchanged and the new backlog protocol exactly matches the reviewed eight-trial plan. Results and plots will be saved for review before proposal insertion. One run per new condition cannot estimate a within-condition sample standard deviation.
