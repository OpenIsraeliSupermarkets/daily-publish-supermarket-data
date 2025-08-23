#!/bin/bash

set -e

# Default values
IMAGE_NAME="erlichsefi/data-fetcher"
PUSH_TO_DOCKERHUB=false
TARGET="data_processing"
TAG="latest"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --push)
            PUSH_TO_DOCKERHUB=true
            shift
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        --tag)
            TAG="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --push              Push image to Docker Hub after building"
            echo "  --target TARGET     Docker build target (default: data_processing)"
            echo "  --tag TAG           Image tag (default: latest)"
            echo "  --help              Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "Building Docker image: $IMAGE_NAME:$TAG (target: $TARGET)"

# Build the Docker image
docker build \
    --target "$TARGET" \
    -t "$IMAGE_NAME:$TAG" \
    .

echo "Successfully built $IMAGE_NAME:$TAG"

# Push to Docker Hub if requested
if [ "$PUSH_TO_DOCKERHUB" = true ]; then
    echo "Pushing $IMAGE_NAME:$TAG to Docker Hub..."
    
    # Check if user is logged in to Docker Hub
    if ! docker info | grep -q "Username"; then
        echo "Error: Not logged in to Docker Hub. Please run 'docker login' first."
        exit 1
    fi
    
    docker push "$IMAGE_NAME:$TAG"
    echo "Successfully pushed $IMAGE_NAME:$TAG to Docker Hub"
else
    echo "Image built locally. Use --push flag to push to Docker Hub."
fi
