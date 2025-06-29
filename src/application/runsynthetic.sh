#!/bin/bash

# Load config
source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)

trap "exit" INT TERM
trap "kill 0" EXIT

# Read pod index from argument
pod_index=${1:-0}
app_count=${data_APPLICATION_COUNT}

input_dir="../consumer-app-data/consumer-result"
output_dir="./application-result"

# Logging setup
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_dir="./logs/application/Pod_${pod_index}/${CURRENT_DATE}/${CURRENT_TIME}"

mkdir -p "${output_dir}"
mkdir -p "${log_dir}"

echo "[Pod $pod_index] Starting application processing for $app_count processes..."

for ((i = 0; i < app_count; i++)); do
    (
        while true; do
            # Find matching files
            matching_files=($(find "$input_dir" -maxdepth 1 -type f -name "consumer_pod${pod_index}_proc${i}_*.csv"))

            if [[ ${#matching_files[@]} -eq 0 ]]; then
                echo "[App $i] No matching file found. Retrying in 5s..."
                sleep 5
                continue
            fi

            # Use the most recent file
            latest_file=$(ls -t "${matching_files[@]}" | head -n 1)

            echo "[App $i] Found file: $latest_file. Starting processing..."
            python3 confluent_application.py --input "$latest_file" --output "$output_dir" --pod_index "$pod_index" --proc_index "$i" & #>& "${log_dir}/application_pod${pod_index}_proc${i}.out" &

            echo "[App $i] Processing complete. Watching for next file..."
        done
    ) &
done

wait

