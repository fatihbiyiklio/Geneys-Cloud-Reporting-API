#!/bin/bash
# Build script for Genesys Cloud Reporting App

set -e

IMAGE_NAME="genesys-cloud-reporting"
TAG="${1:-latest}"

echo "========================================="
echo "Building Genesys Cloud Reporting App"
echo "Image: ${IMAGE_NAME}:${TAG}"
echo "========================================="

# Clean up old __pycache__ directories
echo "Cleaning up cache files..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Build Docker image
echo "Building Docker image..."
docker build -t ${IMAGE_NAME}:${TAG} .

# Show image info
echo ""
echo "========================================="
echo "Build completed successfully!"
echo "========================================="
docker images ${IMAGE_NAME}:${TAG}

echo ""
echo "To run the container:"
echo "  docker run -d -p 8501:8501 --name genesys-reporting ${IMAGE_NAME}:${TAG}"
echo ""
echo "Or using docker-compose:"
echo "  docker-compose up -d"
echo ""
echo "Access the app at: http://localhost:8501"
