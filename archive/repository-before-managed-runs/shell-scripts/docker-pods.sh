#!/bin/bash

# Function to clean up dangling images
cleanup_dangling_images() {
    echo "Cleaning up dangling images..."
    docker rmi $(docker images -f "dangling=true" -q)
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

    # The tagging and pushing steps have been removed/commented out
    # The images will now only exist locally and not be pushed to a remote repository

    # Cleanup dangling images
    cleanup_dangling_images

    echo "Successfully processed ${image_name}:${current_date}"
    return 0
}

# Main script logic
docker_all_pods() {
    # Define an array of pod names
    pod_names=("request" "producer" "consumer" "merge")
    current_date=$(TZ=America/Denver date +"%Y-%m-%d")

    for image_name in "${pod_names[@]}"; do
        if ! process_pod "$image_name" "$current_date"; then
            echo "Not done completely."
            return 1 # Return 1 for error
        fi

        # Special case for consumer pod
        if [[ "$image_name" == "consumer" ]]; then
            if ! process_pod "application" "$current_date"; then
                echo "Not done completely."
                return 1 # Return 1 for error
            fi
        fi
    done

    echo "All pods processed successfully."
    return 0 # Return 0 for success
}

# Call the main function
docker_all_pods

