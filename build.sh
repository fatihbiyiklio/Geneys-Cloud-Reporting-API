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

# Clean Python cache before build context send
find . -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

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

echo ""
echo "Build completed: ${FULL_IMAGE}"
echo ""
echo "Run with docker compose:"
echo "  IMAGE_REPOSITORY=${IMAGE_REPOSITORY} IMAGE_TAG=${IMAGE_TAG} docker compose up -d"
