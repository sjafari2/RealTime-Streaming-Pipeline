# Preparation failure before the larger-backlog retry

Run `run-20261111-101917` did not release a workload. All three producer delivery summaries recorded zero enqueued messages. Consumers 0 and 1 exited during explicit assignment with the client-local `_UNKNOWN_PARTITION` error while requesting watermark offsets. Consumer 2 initialized successfully. The readiness gate rejected the preparation.

Read-only Kafka checks subsequently found all 60 partitions empty and readable from all three consumer pods. These checks did not subscribe, join the experiment group, produce records or commit offsets. The logs establish the initialization error but do not establish its underlying cause. The current assignment retry helper covers related broker metadata errors, but not this client-local error. Source changes are deferred until the frozen campaign has ended.

The failed preparation evidence is retained in the campaign root and in a verified local backup under `backup/matched-backlog600-20261111/readiness-failure`. These copies are on the same physical device. The shared control remained inactive in its `failed` state. All six application processes had stopped. The original shared configuration, consumer template and strategy, 3 replicas, client identities and placement, and broker identities were verified before retry.

The batch resumed with the same runtime revision `8321430676485ac349bbed0328fde8f0f4635b1d`, workload settings and validation rules. The accepted smaller Keep 3 trial is retained. This failed preparation is not counted as a completed main trial.
