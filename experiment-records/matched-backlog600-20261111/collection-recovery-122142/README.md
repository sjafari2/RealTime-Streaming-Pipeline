# Recovered larger-backlog redistribution

Run `run-20261111-122142` finished its original warm-up, evaluation and drain periods. Its coordinated partition handover completed. Kubernetes API calls timed out during final collection and again during the first restoration attempt. Later access confirmed all producer and consumer processes finished without evidence errors.

The original evidence passed the frozen identity, handover, input-rate and monitoring checks. The admitted evaluation input was 600 messages/s. No duplicate message IDs, duplicate offsets or unmatched completions were found. Unfavorable outcomes would still have been retained if valid. No workload setting, timing boundary, source file or validation threshold changed.

The consumer template, update strategy, shared configuration and three replicas were restored and verified before continuation. All collected files were backed up and hash-verified after restoration. The backup is local to the same device and is not independent off-device storage.

The completed run was retained without repetition. The campaign now has four accepted main trials and resumes the remaining four scaling trials using execution revision `8321430676485ac349bbed0328fde8f0f4635b1d`.
