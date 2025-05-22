#This script is to rebuild all images to be converted from arm64 to amd64 compatible nodes
##!/usr/bin/env bash

set -e

# -----------------------------
# Paths
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKERFILES_DIR="${PROJECT_ROOT}/dockerfiles"

# -----------------------------
# Ordered image names
# -----------------------------
IMAGE_NAMES=("base" "request" "producer" "consumer" "application" "merge")

declare -A IMAGE_MAP=(
  ["base"]="sjafari2/kafkabase:latest"
  ["request"]="sjafari2/kafkarequest:latest"
  ["producer"]="sjafari2/kafkaproducer:latest"
  ["consumer"]="sjafari2/kafkaconsumer:latest"
  ["application"]="sjafari2/kafkaapplication:latest"
  ["merge"]="sjafari2/kafkamerge:latest"
)

# -----------------------------
# Install buildx if missing
# -----------------------------
if ! docker buildx version &>/dev/null; then
  echo "🔧 Installing Docker buildx..."
  mkdir -p ~/.docker/cli-plugins
  curl -SL https://github.com/docker/buildx/releases/download/v0.11.2/buildx-v0.11.2.linux-amd64 -o ~/.docker/cli-plugins/docker-buildx
  chmod +x ~/.docker/cli-plugins/docker-buildx
  echo "✅ buildx installed."
else
  echo "✅ Docker buildx already available."
fi

# -----------------------------
# Setup buildx builder
# -----------------------------
if ! docker buildx inspect builder_amd64 &>/dev/null; then
  echo "🔧 Creating buildx builder 'builder_amd64'..."
  docker buildx create --name builder_amd64 --use
else
  echo "🔄 Using existing buildx builder 'builder_amd64'"
  docker buildx use builder_amd64
fi

docker buildx inspect --bootstrap

# -----------------------------
# Build and push images in order
# -----------------------------
cd "$PROJECT_ROOT"

for name in "${IMAGE_NAMES[@]}"; do
  image="${IMAGE_MAP[$name]}"
  dockerfile="${DOCKERFILES_DIR}/${name}.Dockerfile"

  if [[ ! -f "$dockerfile" ]]; then
    echo "⚠️  Skipping $image – Dockerfile not found: $dockerfile"
    continue
  fi

  echo -e "\n🚀 Rebuilding image: $image"
  docker buildx build \
    --platform linux/amd64 \
    -t "$image" \
    -f "$dockerfile" \
    "$PROJECT_ROOT" \
    --push
done

echo -e "\n✅ All images built and pushed for linux/amd64."

