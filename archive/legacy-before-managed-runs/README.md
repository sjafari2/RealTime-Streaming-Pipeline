# Archived files from before managed runs

These files are outside the active workflow. They were moved, not deleted. Do not launch scripts, build images or apply manifests from this directory as part of a managed measurement run. The controller is unfinished research work, not an implemented feature of the active application.

`manifest.json` records every original path, archive path, reason and SHA-256 checksum. The files retain their original contents. Some archived scripts depend on their original directory layout or on other archived files; this is a source archive, not a separately runnable deployment.

To restore a tool later, first inspect its dependencies and copy the relevant files back to their original paths without overwriting current files. The earlier complete source backup is also retained in `backups/before-measurement-update.tar.gz`.

## Replacements

| Old entry point | Active replacement |
|---|---|
| Old one-consumer/run/result scripts | `my-shell/save-run.sh` with one producer and consumer deployed |
| Old force-kill script | `my-shell/save-run.sh stop` for graceful stop and drain |
| Dashboard-based and older Prometheus exporters | `my-shell/save-run.sh export` |
| `target_rates_sweep_save_grafana.sh` | `my-shell/run_pipeline.sh --rates … --repetitions …` |
| `copy_to_consumers.sh` | `my-shell/sync-code.sh --role consumer` |
| Old analysis tools | `python-scripts/evaluate_run.py` for current outcome evidence; historical analyses remain available here |
| Old YAML parsing and producer discovery helpers | `src/common/launch.py` reads the shared configuration |
| Controller prototypes | No replacement yet; adaptive control still needs implementation |

Historical CSV data, dashboard snapshots and node clock diagnostics have not been deleted. Kubernetes/Helm infrastructure files remain outside this archive when their operational use cannot be inferred from Python imports. In particular, `k8s/pvc/pipeline-configmap.yaml` creates the shared volume; it is not a duplicate of the application configuration despite the similar filename.

## Exact moved-file inventory

| Original path | Reason |
|---|---|
| `my-shell/export_grafana_dashboard_to_csv.py` | Superseded launch/export scripts |
| `grafana/export_grafana_dashboard_to_csv.py` | Superseded launch/export scripts |
| `grafana/run_export_grafana.sh` | Superseded launch/export scripts |
| `my-shell/export_prometheus_run.py` | Superseded launch/export scripts |
| `my-shell/export_prometheus_run.sh` | Superseded launch/export scripts |
| `my-shell/one-consumer-save-run.sh` | Superseded launch/export scripts |
| `my-shell/old/results-save-run.sh` | Superseded launch/export scripts |
| `my-shell/old/save-result-log-run.sh` | Superseded launch/export scripts |
| `my-shell/csv-save-grafana.sh` | Superseded launch/export scripts |
| `my-shell/kill-scripts-pods.sh` | Superseded launch/export scripts |
| `my-shell/target_rates_sweep_save_grafana.sh` | Superseded launch/export scripts |
| `my-shell/copy_to_consumers.sh` | Superseded launch/export scripts |
| `src/controller/consumer.properties` | Inactive controller prototype and its deployment |
| `src/controller/create_topics.sh` | Inactive controller prototype and its deployment |
| `src/controller/delete-empty-dirs.py` | Inactive controller prototype and its deployment |
| `src/controller/delete_all_topics.sh` | Inactive controller prototype and its deployment |
| `src/controller/extended_lag_controller.py` | Inactive controller prototype and its deployment |
| `src/controller/extended_run_lag_controller.sh` | Inactive controller prototype and its deployment |
| `src/controller/get_bootstrap_servers.sh` | Inactive controller prototype and its deployment |
| `src/controller/get_kafka_consumer_dns.sh` | Inactive controller prototype and its deployment |
| `src/controller/get_kafka_producer_list.sh` | Inactive controller prototype and its deployment |
| `src/controller/kafka-list-topics.sh` | Inactive controller prototype and its deployment |
| `src/controller/kill-processes.sh` | Inactive controller prototype and its deployment |
| `src/controller/kill-zombie-process.sh` | Inactive controller prototype and its deployment |
| `src/controller/lag_controller.py` | Inactive controller prototype and its deployment |
| `src/controller/parseYaml.sh` | Inactive controller prototype and its deployment |
| `src/controller/run_lag_controller.sh` | Inactive controller prototype and its deployment |
| `dockerfiles_confluent/controller.Dockerfile` | Inactive controller prototype and its deployment |
| `k8s/deployments/controller.yaml` | Inactive controller prototype and its deployment |
| `src/config/consumer.properties` | Old application helpers and configuration copies |
| `src/config/create_topics.sh` | Old application helpers and configuration copies |
| `src/config/delete_all_topics.sh` | Old application helpers and configuration copies |
| `src/config/get_kafka_consumer_dns.sh` | Old application helpers and configuration copies |
| `src/config/get_kafka_producer_dns.sh` | Old application helpers and configuration copies |
| `src/config/kafka-list-topics.sh` | Old application helpers and configuration copies |
| `src/config/pipeline-configmap.yaml` | Old application helpers and configuration copies |
| `src/config/producer.properties` | Old application helpers and configuration copies |
| `dockerfiles_confluent/pipeline-configmap.yaml` | Old application helpers and configuration copies |
| `dockerfiles_confluent/request-requirements.txt` | Old application helpers and configuration copies |
| `src/consumer/parseYaml.sh` | Old application helpers and configuration copies |
| `src/consumer/delete-empty-dirs.py` | Old application helpers and configuration copies |
| `src/consumer/get_kafka_consumer_dns.sh` | Old application helpers and configuration copies |
| `src/consumer/consumer.properties` | Old application helpers and configuration copies |
| `src/consumer/delete_all_topics.sh` | Old application helpers and configuration copies |
| `src/producer/producer.properties` | Old application helpers and configuration copies |
| `src/producer/get_kafka_producer_list.sh` | Old application helpers and configuration copies |
| `src/producer/delete-empty-dirs.py` | Old application helpers and configuration copies |
| `src/producer/parseYaml.sh` | Old application helpers and configuration copies |
| `src/producer/kafka-list-topics.sh` | Old application helpers and configuration copies |
| `src/producer/get_kafka_producer_dns.sh` | Old application helpers and configuration copies |
| `python-scripts/run_collect_parquet.sh` | Historical analysis tools |
| `python-scripts/block_maxima_analysis.ipynb` | Historical analysis tools |
| `python-scripts/move_parquet_files.py` | Historical analysis tools |
| `python-scripts/evt_kafka_latency.py` | Historical analysis tools |
| `python-scripts/list_parquet_files.py` | Historical analysis tools |
| `python-scripts/evt_gev_gpd_analysis.ipynb` | Historical analysis tools |
| `python-scripts/fetch-delete-results-logs.py` | Historical analysis tools |
| `python-scripts/analysis-my.py` | Historical analysis tools |
| `python-scripts/collect_parquet.py` | Historical analysis tools |
| `python-scripts/clean_results.py` | Historical analysis tools |
| `python-scripts/clean_parquet_names.py` | Historical analysis tools |
| `python-scripts/Distribution-Analysis.ipynb` | Historical analysis tools |
| `python-scripts/clean_metrics_plotter.ipynb` | Historical analysis tools |
| `python-scripts/merge-csv.py` | Historical analysis tools |
| `python-scripts/EVT-GPD-Analysis.ipynb` | Historical analysis tools |
| `python-scripts/MyAnalysis.ipynb` | Historical analysis tools |
| `python-scripts/metrics_analysis/metrics_analysis.ipynb` | Historical analysis tools |
| `python-scripts/metrics_analysis/results_metrics/one_file_analysis.py` | Historical analysis tools |
| `python-scripts/metrics_analysis/results_metrics/analisys_all_folders.py` | Historical analysis tools |
| `grafana/Kafka Dashboard– Per-Consumer Metrics-1782177293599.json` | Older dashboard revisions |
| `grafana/Kafka Dashboard– Per-Consumer Metrics-1782179637000.json` | Older dashboard revisions |
