# Twelve-partition comparison at 600 messages/s with Consumer 0

This planned block repeats four responses twice at 600 total messages/s. Twelve of 60 partitions receive 80% of input and initially belong to Consumer 0. Runs use 60 seconds of warm-up, 600 seconds of evaluation and 120 seconds of drain. Mitigation is requested at evaluation second 60. Workload seeds are 81 and 82, with response order reversed in the second repetition.

The responses are Keep 3, redistribution within three consumers, scaling to six with targeted redistribution, and scaling to six with Kafka cooperative rebalancing. The first Keep 3 trial must demonstrate the declared sustained-growth condition before mitigation comparisons proceed. Earlier control `run-20261104-073554` supplies the deployment reference and remains a separate stress-control result, outside this block's eight-trial count.

The [admission plan](admission-plan.json) fixes the reference and stress thresholds. The [monitoring correction gate](monitoring-gate.json) retains its original evidence and date. Fresh readiness checks and a native-scaling technical trial are required before performance runs. The initial consumers, producer placement, resources, processing task and input rate remain fixed within the comparison. Added-consumer placement is recorded.

Raw evidence and live campaign status are stored outside cloud-synchronized directories, with separate verified backups. The wrapper stops each completed application before preserving evidence and stops the batch for failed admission or validation. A valid unfavorable performance result is retained. Existing proposal results are unchanged while this block runs.
