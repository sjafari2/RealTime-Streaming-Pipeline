#!/usr/bin/env bash

set -e

# -----------------------------
# Function: Check if Docker is running
# -----------------------------
check_docker_daemon() {
    if ! docker info >/dev/null 2>&1; then
        echo "🔌 Docker daemon is not running. Trying to start it..."
        
        if command -v systemctl &> /dev/null; then
            sudo systemctl start docker
        elif command -v service &> /dev/null; then
            sudo service docker start
        else
            echo "❌ No known service manager found to start Docker."
            exit 1
        fi

        sleep 3
        if ! docker info >/dev/null 2>&1; then
            echo "❌ Failed to start Docker daemon. Please start it manually."
            exit 1
        else
            echo "✅ Docker daemon started."
        fi
    else
        echo "✅ Docker daemon is running."
    fi
}

# -----------------------------
# Function: Clean up dangling images
# -----------------------------
cleanup_dangling_images() {
    echo "🧹 Cleaning up dangling images..."
    dangling_images=$(docker images -f "dangling=true" -q)

    if [[ -z "$dangling_images" ]]; then
        echo "✔️ No dangling images to clean up."
    else
        docker rmi $dangling_images
        echo "🗑️ Removed: $dangling_images"
    fi
}

# -----------------------------
# Function: Build and push a Docker image
# -----------------------------
process_pod() {
    local image_name="$1"
    local dockerfile_path="dockerfiles/${image_name}.Dockerfile"
    local image_tag="sjafari2/kafka${image_name}:latest"

    # Detect platform and skip if not x86_64
    local arch
    arch=$(uname -m)
    if [[ "$arch" != "x86_64" ]]; then
        echo "⚠️  Host architecture is $arch — skipping build for $image_tag (requires amd64)."
        return 0
    fi

    echo -e "\n🚀 Building ${image_tag} from ${dockerfile_path}"

    if [[ ! -f "$dockerfile_path" ]]; then
        echo "❌ Dockerfile not found: $dockerfile_path"
        return 1
    fi

    # Build image using buildx for linux/amd64
    if ! docker buildx build \
        --platform linux/amd64 \
        -t "$image_tag" \
        -f "$dockerfile_path" \
        . \
        --push; then
        echo "❌ Failed to build and push $image_tag"
        return 1
    fi

    echo "✅ Successfully built and pushed $image_tag"
    cleanup_dangling_images
    return 0
}

# -----------------------------
# Main Script Logic
# -----------------------------
docker_all_pods() {
    check_docker_daemon

    # Ordered pod names (base must be first)
    pod_names=("base" "request" "producer" "consumer" "application" "merge")

    for image_name in "${pod_names[@]}"; do
        if ! process_pod "$image_name"; then
            echo "❌ Build failed for $image_name. Exiting."
            return 1
        fi
    done

    echo -e "\n🎉 All compatible images built and pushed successfully."
    return 0
}

# -----------------------------
# Execute
# -----------------------------
docker_all_pods

