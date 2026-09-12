# Applying the measurement update on Nautilus

The code keeps my shared-file workflow: stop the applications, edit the shared YAML, troubleshoot if needed, and use `my-shell/save-run.sh` to start the experiment. Python source remains on the existing shared producer and consumer volumes. Prometheus stores aggregate measurements. The extra files contain message outcomes, configuration, ownership events and final metric snapshots.

The updated application runtime was synchronized and exercised on Nautilus during the 12 September 2026 preliminary campaign, including scheduled three-to-six consumer increases. See the [campaign report](../experiment-records/campaign-20260912/campaign-report.md) for exact run evidence and limits. The prepared dynamic Prometheus discovery update was rejected during deployment; the existing static six-consumer/three-producer targets were used with 5-second application scrapes. Broker monitoring remains incomplete. These setup instructions remain useful after future source or deployment changes; do not repeat the whole setup before every run. The original local files are preserved in `backups/before-measurement-update.tar.gz`.

## Which steps I run once and which I repeat

Complete the source/dependency, shared configuration, consumer startup and monitoring setup sections once for the first update. Repeat source sync after changing runtime source, and change workload settings between runs as needed. These are setup instructions, not a list to execute from top to bottom for every experiment. Existing monitoring must be checked before applying any monitoring manifests.

After setup, use `bash my-shell/save-run.sh` for one complete run or `bash my-shell/run_pipeline.sh --repetitions 5` for five repetitions. These commands wait, collect, export and analyze automatically. The separate `start`, `status`, `stop`, `collect` and `export` actions remain available for troubleshooting.

## What changed

- Completion latency is now measured after the configured waiting/CPU work. Processing-start latency remains a separate diagnostic. The old `consumer_e2e_latency_seconds` name now means **completion latency**; do not combine earlier runs with new ones as the same measurement.
- Offset storage is explicit and commits use the sequential completion frontier. Invalid records stop the run with a recorded error instead of being skipped and committed past. Ownership callbacks remove revoked lag series; a lost owner does not commit.
- Lag queries have a total time budget, rotate through partitions and expose observation time/validity. Missing or expired observations produce NaN rather than zero. The dashboard checks full partition coverage and unique ownership.
- Both roles read one frozen snapshot of the shared YAML for a managed run. Editing the shared original between runs still works as before. The coordinator checks configuration hashes and assignment coverage before publishing a common start time.
- Producers stop at the shared production end. Consumers continue to the shared drain bound. Normal exit and SIGTERM run cleanup. Final metrics remain available for several scrapes and are also written to durable storage.
- The consumer supervisor starts the application when a run is active, including in newly added replicas. Idle consumer pods wait for a run. A per-pod file lock prevents the supervisor and the manual launcher from starting two consumers.
- The prepared Prometheus configuration supports dynamic pod discovery, but that configuration is not deployed in the recorded campaign. The live static targets cover its three-to-six consumer comparisons. The exporter selects the exact run, retains every label and NaN value, and reports if any process incarnation has no Prometheus series.
- `save-run.sh` remains the entry point but delegates to a Python coordinator. This removes the Bash associative-array dependency on the local Mac. Backups downloaded from Nautilus go to `backups/`, never over edited local source. Topic deletion and result deletion are no longer automatic.
- Producer client settings are wired into librdkafka. ACKS=0 is rejected because it cannot establish the acknowledged-message cohort. Hot/cold routing is explicit and seeded; the arrival counter uses actual broker-acknowledged partition IDs.

The older controller prototypes and analysis notebooks are preserved under `archive/legacy-before-managed-runs/`. This update does not implement targeted reassignment, tune an autoscaling decision policy, or claim a validated stateful processing application. The focus is the measurement and run-management changes discussed after the review. CPU work is repeated SHA-256; waiting work uses APP_DELAY_MS. The output hash is evidence of the synthetic task, not a transactional downstream sink.

## First update of existing shared volumes

Run commands from the local code directory. Confirm that kubectl points to Nautilus and your intended namespace.

```bash
cd /Users/soheila/Desktop/Thesis-26-27/code
kubectl config current-context
bash my-shell/save-run.sh backup
bash my-shell/save-run.sh stop
bash my-shell/sync-code.sh
```

The first stop also supports the old producer/consumer process names. For an older run without a managed manifest, it stops producers, waits `LEGACY_DRAIN_SECONDS` (default 10), then stops consumers. This is a bounded transition step, not retrospective proof that the old run fully drained. If a force kill is needed after the grace period, the command fails and reports the affected pods. Preserve the old experiment's results separately before migrating.

`sync-code.sh` checks that application processes are stopped, copies the edited Python/launch files once per role's shared PVC, then verifies their hashes in every current replica. It does not overwrite `/config/pipeline-configmap.yaml`. It also does not rebuild images or change the broker deployment.

Check imports inside one pod of each role:

```bash
kubectl -n kafkastreamingdata exec consumer-sts-0 -c consumer-container -- \
  python3 -c 'import confluent_kafka, prometheus_client, psutil, yaml; print(confluent_kafka.version(), confluent_kafka.libversion())'

kubectl -n kafkastreamingdata exec producer-sts-0 -c producer-container -- \
  python3 -c 'import confluent_kafka, prometheus_client, psutil, yaml; print(confluent_kafka.version(), confluent_kafka.libversion())'
```

The local tests used confluent-kafka 2.15.1. `dockerfiles_confluent/runtime-requirements.txt` records the tested runtime packages. The Dockerfiles include those dependencies and the common launch files, and no longer reference missing `experiments.yaml`. Existing base image tags are retained; record their actual digests before accepted performance runs. An existing image can be used for the smoke test if its installed client supports the configuration; report an import/configuration error rather than guessing compatibility.

## Update the shared configuration once

Compare the shared YAML with the new local `src/pipeline-configmap.yaml`. Keep your cluster's bootstrap address and other intentional settings. Add the new keys and remove unused CSV options when convenient. The important initial values are:

| Setting | Initial value | Purpose |
|---|---|---|
| ACKS | all | Obtain broker acknowledgment |
| ENABLE_IDEMPOTENCE | true | Consistent producer retry behavior |
| MAX_IN_FLIGHT_REQUEST_PER_CONNECTION | 5 | Compatible with idempotence |
| RETENTION_MS | 86400000 | Avoid deleting the experimental backlog |
| DRAIN_SECONDS | 60 | Continue consumption after production stops |
| FINAL_SCRAPE_SECONDS | 15 | Keep final counters observable |
| PRODUCER_FLUSH_SECONDS | 30 | Bound delivery resolution at stop |
| READINESS_TIMEOUT_SECONDS | 180 | Fail a run that never becomes ready |
| WORKLOAD_SEED | 1 | Reproduce partition routing across paired runs |
| LAG_QUERY_TIMEOUT | 0.1 | Bound an individual query |
| LAG_QUERY_BUDGET_SEC | 0.2 | Bound one iteration's instrumentation work |
| LAG_FRESHNESS_SECONDS | 10 | Identify stale partition samples |
| OUTCOME_LOG_ENABLED | true | Retain identities for cohort/correctness checks |
| EVIDENCE_MAX_BUFFER | 100000 | Bound asynchronous outcome buffering |
| PRODUCER_EVIDENCE_DIR | /app/producer-data/evidence | Producer shared persistent storage |
| CONSUMER_EVIDENCE_DIR | /app/consumer-merge-data/evidence | Consumer shared persistent storage |

`EXP_DURATION_SEC` is the entire production interval. `WARMUP_SECONDS` excludes its initial part from the admitted evaluation cohort. For the proposal's pilot, a possible schedule is 1500 seconds of production with 300 seconds of warm-up; a controlled intervention would occur after a further 300 seconds. Optional scheduled no-action and scale-up pilots are now supported; targeted reassignment and the adaptive selector remain planned. The default 300-second run is for development, not the complete thesis pilot.

The shared `run-control.json` is managed by the coordinator. Do not edit it during a measurement run. A frozen YAML and manifest are stored under `/config/runs/RUN_ID/`. Runtime configuration hashes must agree. Directly editing the original shared YAML while a managed run is active does not alter its frozen configuration.

## Enable automatic startup for newly added consumers

The revised consumer StatefulSet runs `supervise.py` instead of `tail -f /dev/null`. Run these commands from the local code folder between experiments. Run each command only after the previous one succeeds. Stopping and syncing first ensures that the supervisor and its supporting files are on the shared volumes before pods restart.

```bash
cd /Users/soheila/Desktop/Thesis-26-27/code
bash my-shell/save-run.sh stop
bash my-shell/sync-code.sh
```

Check the live update strategies:

```bash
kubectl -n kafkastreamingdata get statefulset consumer-sts producer-sts -o custom-columns='NAME:.metadata.name,STRATEGY:.spec.updateStrategy.type,PARTITION:.spec.updateStrategy.rollingUpdate.partition'
```

The rollout commands below assume `RollingUpdate` with partition `0` (an omitted partition, shown as `<none>`, also defaults to zero). If either resource uses `OnDelete` or a nonzero partition, resolve that deliberate rollout policy before proceeding; do not assume every existing pod will update automatically.

Apply the startup and shutdown changes to the existing StatefulSets:

```bash
kubectl -n kafkastreamingdata patch statefulset consumer-sts --type=strategic -p '{"spec":{"template":{"spec":{"terminationGracePeriodSeconds":120,"containers":[{"name":"consumer-container","command":["python3"],"args":["/app/consumer-merge-data/supervise.py"]}]}}}}'
kubectl -n kafkastreamingdata rollout status statefulset/consumer-sts --timeout=300s

kubectl -n kafkastreamingdata patch statefulset producer-sts --type=strategic -p '{"spec":{"template":{"spec":{"terminationGracePeriodSeconds":120}}}}'
kubectl -n kafkastreamingdata rollout status statefulset/producer-sts --timeout=300s
```

These strategic patches set the consumer startup command and both termination grace periods. They preserve the live replica counts, images, placement, resource settings and shared mounts. Updating the pod template normally restarts existing pods under RollingUpdate, so complete this step before starting another experiment. A timeout requires checking pod status; it does not undo the patch. Do not delete `.code_initialized` or the shared volumes to make this change.

Confirm that an updated consumer runs the supervisor as its main process:

```bash
kubectl -n kafkastreamingdata exec consumer-sts-0 -c consumer-container -- python3 -c 'from pathlib import Path; print(Path("/proc/1/cmdline").read_bytes().replace(bytes([0]), b" ").decode())'
```

Expected output contains `python3 /app/consumer-merge-data/supervise.py`. With no active run, the supervisor waits; an idle pod does not mean a consumer application is already processing. When `save-run.sh` publishes a managed run, the supervisor starts `run.sh`. A newly added consumer pod follows the same path while that run is active. This enables automatic application startup; it does not implement automatic scaling.

References: [Kubernetes strategic patches](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/) and [StatefulSet update strategies](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#update-strategies).

## Enable dynamic Prometheus discovery

These files target the standalone Prometheus deployment named `prometheus` in the supplied code. If Nautilus uses a Prometheus Operator installation instead, retain that deployment and use its working ServiceMonitor path; do not introduce a second scraper accidentally.

For the supplied standalone deployment:

```bash
kubectl -n kafkastreamingdata apply -f k8s/serviceaccount/prometheus-discovery.yaml
kubectl -n kafkastreamingdata apply -f k8s/grafana-prometheus/prometheus-configmap.yaml
kubectl -n kafkastreamingdata apply -f k8s/grafana-prometheus/prometheus-deployment.yaml
kubectl -n kafkastreamingdata rollout restart deployment/prometheus
kubectl -n kafkastreamingdata rollout status deployment/prometheus --timeout=300s
```

The restart is needed because the existing deployment mounts configuration files through `subPath`. Import `grafana/KafkaDashboardPerConsumerMetrics.json` in Grafana and select the Prometheus data source and run ID. The primary export no longer depends on dashboard queries.

Start your existing Prometheus port-forward before export. `PROM_URL` defaults to `http://localhost:9090` and can be overridden.

For a detailed file map and troubleshooting steps, see [runtime and data flow](runtime-and-data-flow.md).

## Normal run commands

Run these on the Mac or other machine with the Nautilus kubectl context, from the code folder. Choose one command for the task; do not execute every example sequentially.

One complete experiment using the current shared configuration:

```bash
cd /Users/soheila/Desktop/Thesis-26-27/code
bash my-shell/save-run.sh
```

Five repetitions of that configuration:

```bash
bash my-shell/run_pipeline.sh --repetitions 5
```

Five repetitions at each of three target rates (15 runs in total):

```bash
bash my-shell/run_pipeline.sh --rates 1000 2000 3000 --repetitions 5
```

The rates are per producer. Without `--rates`, the shared target rate is retained. With `--rates`, the coordinator updates TARGET_RATE between runs and leaves the last rate in the shared YAML. The scripts discover actual pods; configured pod-count values do not scale StatefulSets.

Each complete run checks dependencies and shared source hashes, uses the existing Prometheus service, stops previous applications, creates fresh topics, freezes configuration, starts both roles and waits for readiness. It then waits for the full production interval, bounded drain and final snapshots before collecting evidence, exporting Prometheus and running the outcome and lag analyzers. Without explicit pilot options, it does not install packages, copy source, edit other workload settings, scale replicas or delete old topics. Those changes are deliberate setup/configuration steps.

By default, the scripts open their own temporary local port-forward to `prometheus-svc:9090` in the workload namespace and close only that forward when done. No second terminal is needed for export. Existing forwards are left alone. An existing `PROM_URL` is used directly; `PROM_NAMESPACE`, `PROM_SERVICE` and `PROM_SERVICE_PORT` can select a different existing service. Grafana remains available for viewing but is not needed for export.

A complete single run writes `outcome-summary.json`, `lag-summary.json` and `runner-status.json` in its `results/RUN_ID/` folder alongside raw evidence and metrics. The repeated command saves a batch folder under `results/batches/BATCH_ID/`, with the exact completed run paths in `batch-status.json` and a configuration-grouped `repetition-summary.json` after the batch completes. It does not mix unrelated folders into that summary. Check lag coverage and scientific validity even when the runner reports completion.

A failure stops the sequence; collected files remain available. An export failure still allows outcome analysis from persistent evidence. Ctrl+C during startup or waiting attempts to stop/drain the run created by that invocation and save evidence. Allow cleanup to finish; another interrupt or closing the terminal can prevent collection. Interrupted runs are marked in runner status and are not added to the automatic batch summary. Only one complete-run command may hold the local folder's lock; do not launch another coordinator from elsewhere or change the shared configuration during a batch.

For troubleshooting only, use individual actions. `start` returns after readiness, and `stop` ends production early; running them immediately one after another would shorten the experiment. `export` and `collect` operate on the current shared run control. To retain the earlier prompt-driven workflow, use `bash my-shell/save-run.sh interactive`.

```bash
bash my-shell/save-run.sh status
```

## One producer and one consumer for troubleshooting

Keep using a small deployment when testing changes. For coordinated measurement with one of each, scale/select your test deployment accordingly and use save-run.sh. Its manifest records the actual pods; the configured pod counts are descriptive and do not scale the resources automatically.

For an intentionally manual process test in a pod, after stopping the managed run and creating suitable topics:

```bash
PIPELINE_STANDALONE=true bash /app/consumer-merge-data/run.sh
```

Then in the producer pod:

```bash
PIPELINE_STANDALONE=true bash /app/producer-data/run.sh
```

Standalone mode reads the shared YAML afresh at each start and uses local process timing. It is for debugging, not a synchronized comparison. Never run it concurrently with a managed experiment.

## Files retained after a run

`results/RUN_ID/` contains:

- `manifest.json` and `pipeline-configmap.yaml`: actual run schedule, initial pods, config hash and coarse clock probes.
- `prometheus.json` and `prometheus.csv`: raw run-scoped time series with complete labels. Missing/NaN values are not converted to zero.
- `export-status.json`: whether Prometheus observed every process incarnation present in collected final records.
- `producer/` and `consumer/`: one directory per pod/incarnation, containing asynchronous `events.jsonl`, `final.json` and `final.prom`.
- `outcome-summary.json`: generated by evaluate_run.py; includes admitted cohort, incomplete messages, deadline misses, duplicates and empirical completion p99.

The writer does not synchronously flush a CSV for every record. It queues compact outcome records, writes them on a separate thread, and flushes durably at close. Buffer loss or write failure invalidates the run. Measure the overhead during calibration. Set OUTCOME_LOG_ENABLED=false only for an explicitly metrics-only diagnostic; the evaluator will flag the absence of correctness evidence.

The histogram p99 and empirical cohort p99 have different populations/windows. The dashboard is a rolling completion view. The evaluator follows messages admitted in the common evaluation interval through drain. Neither a mean of rolling p99 values nor a mean of consumer p99 values is the run's combined p99.

## Nautilus smoke test to send back

Use a short balanced run at a low target rate first. Set APP_DELAY_MS=50 to check timing; use a sufficiently low rate per producer to avoid making this first test an overload run. Keep DRAIN_SECONDS at least as long as both the deadline and producer flush timeout.

1. Run `save-run.sh start` and send the output from `save-run.sh status`. All pods should have the same run/config hash, and consumer assignments should cover each topic partition once.
2. Let the run finish, export and run evaluate_run.py. Send `outcome-summary.json` and `export-status.json`. Check that completion latency includes approximately 50 ms more than processing-start latency.
3. In another run, add one consumer using `kubectl scale statefulset consumer-sts --replicas=7 -n kafkastreamingdata` (use 2 for a one-consumer test). The new pod should start automatically, acquire partitions and appear in Prometheus. Restore the intended baseline between runs.
4. Test a scale-down/SIGTERM during processing. Verify final evidence for the removed incarnation remains on shared storage, no unexplained missing outcomes occur, and any replay appears in duplicate-attempt accounting.
5. Check Prometheus targets and lag validity. Repeated stale samples mean query timeout/freshness settings need calibration on the real cluster.

The manifest's clock probes bracket pod clock offsets using kubectl round trips. Those intervals include command overhead and are only coarse diagnostics; they do not establish millisecond-level synchronization. Report them alongside your existing node/NTP checks before interpreting a tight latency deadline.

The local verification uses mocked Kafka boundaries plus real subprocess/SIGTERM and file-writing tests. It does not establish live Kafka rebalance behavior, storage performance, CPU capacity, node clock accuracy or Kubernetes permissions. Those are the reasons for the Nautilus smoke test.

The expanded p50/p95/p99, partition and execution summaries apply to both run commands. See [scheduled pilots and new results](runtime-and-data-flow.md#10-scheduled-pilots-and-new-results) for optional action flags, resource coverage, baseline restoration and remaining limitations. Synchronize the updated consumer code between experiments before using this version.
