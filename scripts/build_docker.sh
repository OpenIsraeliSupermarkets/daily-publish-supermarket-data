#!/bin/bash

set -e

# Default values
IMAGE_NAME="erlichsefi/data-fetcher"
PUSH_TO_DOCKERHUB=false
TARGET="data_processing"
TAG="latest"
REGISTRY=""

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
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --push              Push image to registry after building"
            echo "  --target TARGET     Docker build target (default: data_processing)"
            echo "  --tag TAG           Image tag (default: latest)"
            echo "  --registry REGISTRY Registry URL (e.g., ghcr.io/username/repo)"
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

# Set full image name with registry if provided
if [ -n "$REGISTRY" ]; then
    FULL_IMAGE_NAME="$REGISTRY:$TAG"
else
    FULL_IMAGE_NAME="$IMAGE_NAME:$TAG"
fi

echo "Building Docker image: $FULL_IMAGE_NAME (target: $TARGET)"

# Build the Docker image
docker build \
    --target "$TARGET" \
    -t "$FULL_IMAGE_NAME" \
    .

echo "Successfully built $FULL_IMAGE_NAME"

# Push to registry if requested
if [ "$PUSH_TO_DOCKERHUB" = true ]; then
    echo "Pushing $FULL_IMAGE_NAME to registry..."
    
    # Check if user is logged in to registry
    if ! docker info | grep -q "Username"; then
        echo "Error: Not logged in to registry. Please run 'docker login' first."
        exit 1
    fi
    
    docker push "$FULL_IMAGE_NAME"
    echo "Successfully pushed $FULL_IMAGE_NAME to registry"
else
    echo "Image built locally. Use --push flag to push to registry."
fi
