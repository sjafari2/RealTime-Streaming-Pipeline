#!/usr/bin/env bash

set -e

# -----------------------------
# Input check
# -----------------------------
if [ $# -ne 1 ]; then
  echo "Usage: $0 <image_name> (e.g., request)"
  exit 1
fi

# -----------------------------
# Variables
# -----------------------------
IMAGE_NAME="$1"
TAG="final"
REPO="sjafari2"
FULL_IMAGE="${REPO}/kafka${IMAGE_NAME}:${TAG}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKERFILES_DIR="${PROJECT_ROOT}/dockerfiles"
DOCKERFILE="${DOCKERFILES_DIR}/${IMAGE_NAME}.Dockerfile"

# -----------------------------
# Check Dockerfile exists
# -----------------------------
if [[ ! -f "$DOCKERFILE" ]]; then
  echo "Dockerfile not found: $DOCKERFILE"
  exit 1
fi

# -----------------------------
# Setup buildx
# -----------------------------
if ! docker buildx version &>/dev/null; then
  echo "Installing Docker buildx..."
  mkdir -p ~/.docker/cli-plugins
  curl -SL https://github.com/docker/buildx/releases/download/v0.11.2/buildx-v0.11.2.linux-amd64 -o ~/.docker/cli-plugins/docker-buildx
  chmod +x ~/.docker/cli-plugins/docker-buildx
  echo "buildx installed."
fi

if ! docker buildx inspect builder_amd64 &>/dev/null; then
  echo "Creating buildx builder 'builder_amd64'..."
  docker buildx create --name builder_amd64 --use
else
  echo "Using existing buildx builder 'builder_amd64'"
  docker buildx use builder_amd64
fi

docker buildx inspect --bootstrap

# -----------------------------
# Build and push
# -----------------------------
cd "$PROJECT_ROOT"

echo "Building image: $FULL_IMAGE"
docker buildx build \
  --platform linux/amd64 \
  -t "$FULL_IMAGE" \
  -f "$DOCKERFILE" \
  "$PROJECT_ROOT" \
  --push

echo "Image built and pushed: $FULL_IMAGE"

