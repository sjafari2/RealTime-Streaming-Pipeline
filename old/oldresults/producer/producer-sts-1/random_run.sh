#!/bin/bash

# Load configuration from YAML
source parseYaml.sh
eval $(parse_yaml pipeline-configmap.yaml)

# Trap signals to gracefully exit or clean up
trap "exit" INT TERM
trap "kill 0" EXIT

# Extract configuration values
nproducers=${data_PRODUCER_COUNT}
num_topics=${data_PRODUCER_TOPIC_COUNT}
pod_count=${data_PRODUCER_POD_COUNT}
input_path=${data_PRODUCER_INPUT_PATH}
batch_size=${data_BATCH_SIZE}
wait_time=${data_WAITE_TIME}
topic_title=${data_TOPIC_TITLE}
col_range=${data_Column_Range}
pod_index=${1:-0}

# Resolve Kafka broker DNS
chmod +x get_kafka_producer_dns.sh
server_uri=$(bash ./get_kafka_producer_dns.sh)

# Convert variables for convenience
np=$nproducers
nt=$num_topics
pi=$pod_index
bs=$batch_size
wt=$wait_time

# Create log directory
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/producer/${topic_title}/${CURRENT_DATE}/${CURRENT_TIME}/Pod_$pi"
mkdir -p "${log_path}"

# Kill any existing producer processes
kill_existing_processes() {
    if pgrep -f mainProducer.py > /dev/null; then
        echo "Killing existing mainProducer.py processes..."
        pkill -f mainProducer.py
    fi
}
kill_existing_processes

# Main loop: wait for input and run producers
#while true; do
       for ((i = 0; i < np; i++)); do
            echo "Running Producer[$i]"
            python3 mainProducer.py \
                -topicTitle "$topic_title" \
                -np "$np" \
                -nt "$nt" \
                -pi "$pi" \
                -pri "$i" \
                -inputpath "$input_path" \
                -bs "$bs" \
                -wtime "$wt" \
                -cr "$col_range" \
                -uris "$server_uri" 
               # >& "${log_path}/producer.$i.$((np - 1)).out" &
            pids[$i]=$!
        done

        echo "All mainProducer.py instances started."

        # Wait for all producer processes to finish
        for pid in "${pids[@]}"; do
            wait "$pid"
        done

        echo " All producer processes completed."
#done


