# Retained invalid smaller-backlog attempt

Run run-20261111-090854 failed the existing identity-consistency gate before admission. One logical message was completed at Kafka offsets 1082 and 1114 in partition 13. An independent read of both broker records confirmed identical message ID and creation-time headers. Producer idempotence was enabled in both the frozen configuration and producer event record. The exact producer, broker or transport cause is unconfirmed.

All raw evidence is retained. A verified complete backup was made after observers stopped. The earlier backup is also retained. Its resource history was an exact prefix of the final file and lacked a final observer append. No accepted performance results were discarded, and no scientific or validation settings were changed. The same smaller-backlog Keep 3 condition is retried after verified restoration. Calibration and successful technical readiness checks remain retained.
