#!/bin/bash

# Function to check if Docker daemon is running
check_docker_daemon() {
    if ! docker info >/dev/null 2>&1; then
        echo "Docker daemon is not running. Trying to start it..."
        
        # Try systemctl
        if command -v systemctl &> /dev/null; then
            sudo systemctl start docker
        # Try service as fallback
        elif command -v service &> /dev/null; then
            sudo service docker start
        else
            echo "No known service manager found to start Docker."
            exit 1
        fi

        # Re-check after attempting to start
        sleep 3
        if ! docker info >/dev/null 2>&1; then
            echo "Failed to start Docker daemon. Please start it manually."
            exit 1
        else
            echo "Docker daemon started successfully."
        fi
    else
        echo "Docker daemon is running."
    fi
}

# Function to clean up dangling images
cleanup_dangling_images() {
    echo "Cleaning up dangling images..."
    dangling_images=$(docker images -f "dangling=true" -q)

    if [[ -z "$dangling_images" ]]; then
        echo "No dangling images to clean up."
    else
        docker rmi $dangling_images
    fi
}

# Function to process each pod
process_pod() {
    local image_name="$1"
    local current_date="$2"

    echo "Creating docker image for $image_name"

    # Build Docker image
    if ! docker build -t "${image_name}:${current_date}" -f dockerfiles/"${image_name}.Dockerfile" .; then
        echo "Error building ${image_name}:${current_date}"
        return 1
    fi

    # Tag Docker image
    if ! docker tag "${image_name}:${current_date}" "sjafari2/kafka${image_name}:latest"; then
        echo "Error tagging ${image_name}:${current_date}"
        return 1
    fi

    # Push Docker image
    if ! docker push "sjafari2/kafka${image_name}:latest"; then
        echo "Error pushing sjafari2/kafka${image_name}:latest"
        return 1
    fi

    # Cleanup dangling images
    cleanup_dangling_images

    echo "Successfully processed ${image_name}:${current_date}"
    return 0
}

# Main script logic
docker_all_pods() {
    # Check Docker daemon status
    check_docker_daemon

    # Define an array of pod names
    pod_names=("base" "request" "producer" "consumer" "merge")
    current_date=$(TZ=America/Denver date +"%Y-%m-%d")

    for image_name in "${pod_names[@]}"; do
        if ! process_pod "$image_name" "$current_date"; then
            echo "Not done completely."
            return 1
        fi

        # Special case for consumer pod
        if [[ "$image_name" == "consumer" ]]; then
            if ! process_pod "application" "$current_date"; then
                echo "Not done completely."
                return 1
            fi
        fi
    done

    echo "All pods processed successfully."
    return 0
}

# Call the main function
docker_all_pods

