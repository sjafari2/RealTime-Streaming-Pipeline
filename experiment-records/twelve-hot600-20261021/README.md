# Twelve-partition comparison at 600 messages/s: preparation

The October 21 attempt passed two empty-topic preparations: initial ownership capture and matching restart. The subsequent low-load native-scaling check failed before the eight planned performance trials began. **No performance trials were completed.**

Consumer 3 could not be scheduled within the required UCSD node pool while maintaining separate nodes for all producer and consumer pods. Scheduler events identified insufficient requested CPU/memory on candidate nodes and conflicts with the placement rules. Consumer 4 also missed the readiness gate, although it became ready later. The attempt is a failed technical preparation, not a measured performance outcome.

The application control was stopped, available run evidence and monitoring were collected, and the original shared configuration was restored and verified by SHA-256. Both client StatefulSets were then scaled to zero and verified at 0/0. Persistent storage was retained.

[Preparation record](preparation.json) identifies the execution revision, checks and evidence hashes. Raw evidence remains under `results/four-condition-20261021-202427` and `results/run-20261021-203056` in separately retained storage. The intended settings remain in the [experiment protocol](../../experiments/twelve-hot600-20261021/protocol.json). Retry requires available capacity and passing the preparation gates; it does not justify silently reducing resource requests or relaxing separation.
