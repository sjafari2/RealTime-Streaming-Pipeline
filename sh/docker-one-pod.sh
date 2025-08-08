#!/bin/bash

set -e

# -----------------------------
# Input validation
# -----------------------------
if [ $# -ne 1 ]; then
  echo "Usage: $0 <image_name> (e.g., request)"
  exit 1
fi

IMAGE_NAME="$1"
REPO="sjafari2"
LATEST_TAG="${REPO}/kafkaconfluent${IMAGE_NAME}:latest"
DOCKERFILE="dockerfiles_confluent/${IMAGE_NAME}.Dockerfile"

# -----------------------------
# Verify Dockerfile exists
# -----------------------------
if [[ ! -f "$DOCKERFILE" ]]; then
  echo "Dockerfile not found: $DOCKERFILE"
  exit 1
fi

# -----------------------------
# Ensure buildx is ready
# -----------------------------
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

# -----------------------------
# Build and push latest
# -----------------------------
echo "Building and pushing multi-arch image: $LATEST_TAG"
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "$LATEST_TAG" \
  -f "$DOCKERFILE" \
  . \
  --push

echo "Image successfully built and pushed: $LATEST_TAG"

