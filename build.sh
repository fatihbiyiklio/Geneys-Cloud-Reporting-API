#!/bin/bash
# Production docker build helper

set -euo pipefail

IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-ghcr.io/fbiyikli/genesys-cloud-reporting}"
IMAGE_TAG="${1:-latest}"
PUSH_IMAGE="${PUSH_IMAGE:-0}"
PLATFORM="${PLATFORM:-linux/amd64}"

FULL_IMAGE="${IMAGE_REPOSITORY}:${IMAGE_TAG}"

echo "========================================="
echo "Building Docker image"
echo "Image: ${FULL_IMAGE}"
echo "Platform: ${PLATFORM}"
echo "Push: ${PUSH_IMAGE}"
echo "========================================="

# Optional preflight: catch syntax errors before Docker build
if command -v python3 >/dev/null 2>&1; then
  echo "Running Python syntax preflight..."
  python3 -m compileall -q app.py run_app.py src
fi

if docker buildx version >/dev/null 2>&1; then
  echo "Using docker buildx"
  if [ "${PUSH_IMAGE}" = "1" ]; then
    docker buildx build \
      --platform "${PLATFORM}" \
      --pull \
      -t "${FULL_IMAGE}" \
      --push \
      .
  else
    docker buildx build \
      --platform "${PLATFORM}" \
      --pull \
      -t "${FULL_IMAGE}" \
      --load \
      .
  fi
else
  echo "docker buildx not found, falling back to docker build"
  docker build \
    --pull \
    -t "${FULL_IMAGE}" \
    .
  if [ "${PUSH_IMAGE}" = "1" ]; then
    docker push "${FULL_IMAGE}"
  fi
fi

echo ""
echo "Build completed: ${FULL_IMAGE}"
echo ""
echo "Run with docker compose:"
echo "  IMAGE_REPOSITORY=${IMAGE_REPOSITORY} IMAGE_TAG=${IMAGE_TAG} docker compose up -d"
