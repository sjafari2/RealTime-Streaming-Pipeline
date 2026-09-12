#!/bin/bash

set -e

# -----------------------------
# Function: Check Docker daemon
# -----------------------------
check_docker_daemon() {
    if ! docker info >/dev/null 2>&1; then
        echo "Docker daemon is not running. Trying to start it..."

        if command -v systemctl &> /dev/null; then
            sudo systemctl start docker
        elif command -v service &> /dev/null; then
            sudo service docker start
        else
            echo "No known service manager found to start Docker."
            exit 1
        fi

        sleep 3
        if ! docker info >/dev/null 2>&1; then
            echo "Failed to start Docker daemon. Please start it manually."
            exit 1
        fi
    fi
}

# -----------------------------
# Function: Clean dangling images
# -----------------------------
cleanup_dangling_images() {
    echo "Cleaning up dangling images..."
    dangling_images=$(docker images -f "dangling=true" -q)

    if [[ -n "$dangling_images" ]]; then
        docker rmi $dangling_images
    else
        echo "No dangling images to clean up."
    fi
}

# -----------------------------
# Function: Build and push image
# -----------------------------
process_pod() {
    local image_name="$1"
    local repo="sjafari2"
    local full_tag="${repo}/kafka${image_name}:latest"
    local dockerfile="dockerfiles/${image_name}.Dockerfile"

    echo "Building and pushing multi-arch image for $image_name"

    if [[ ! -f "$dockerfile" ]]; then
        echo "Dockerfile not found: $dockerfile"
        return 1
    fi

    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        -t "$full_tag" \
        -f "$dockerfile" \
        . \
        --push

    cleanup_dangling_images

    echo "Successfully built and pushed $full_tag"
    return 0
}

# -----------------------------
# Main Logic
# -----------------------------
docker_all_pods() {
    check_docker_daemon

    # Ensure buildx builder exists
    if ! docker buildx version &>/dev/null; then
        echo "Installing Docker buildx..."
        mkdir -p ~/.docker/cli-plugins
        curl -SL https://github.com/docker/buildx/releases/download/v0.11.2/buildx-v0.11.2.linux-amd64 -o ~/.docker/cli-plugins/docker-buildx
        chmod +x ~/.docker/cli-plugins/docker-buildx
    fi

    if ! docker buildx inspect builder_multiarch &>/dev/null; then
        docker buildx create --name builder_multiarch --use --driver docker-container
    else
        docker buildx use builder_multiarch
    fi

    docker buildx inspect --bootstrap

    pod_names=("base" "request" "producer" "consumer" "merge")

    for image_name in "${pod_names[@]}"; do
        if ! process_pod "$image_name"; then
            echo "Failed to process image: $image_name"
            return 1
        fi

        if [[ "$image_name" == "consumer" ]]; then
            if ! process_pod "application"; then
                echo "Failed to process image: application"
                return 1
            fi
        fi
    done

    echo "All images built and pushed successfully."
    return 0
}

# -----------------------------
# Execute
# -----------------------------
docker_all_pods

