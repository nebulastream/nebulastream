#!/bin/bash

# This script builds a Docker image from a Dockerfile.
# Per default, the script builds the Docker image with the name "nes-public-build-image" and the tag "latest" from the Dockerfile located in the "./buildImage" directory.
# The user can specify the image name, tag, and the path to the Dockerfile as arguments.
# To build the dev image, run the script with the following command:
# bash scripts/build_docker_image_non_executable_image.sh nes-public-dev-image latest ./devImage Dockerfile-NES-Dev


# Default values for arguments
IMAGE_NAME_DEFAULT="nes-public-build-image"
TAG_DEFAULT="latest"
DOCKERFILE_PATH_DEFAULT="./buildImage"
DOCKER_FILE_NAME_DEFAULT="Dockerfile-NES-Build"

# Assign arguments with default fallbacks
IMAGE_NAME="${1:-$IMAGE_NAME_DEFAULT}"
TAG="${2:-$TAG_DEFAULT}"
DOCKERFILE_PATH="${3:-$DOCKERFILE_PATH_DEFAULT}"
DOCKER_FILE_NAME="${4:-$DOCKER_FILE_NAME_DEFAULT}"

# Getting for all paths the absolute path
DOCKERFILE_PATH=$(realpath $DOCKERFILE_PATH)

# Printing out all the arguments
echo "IMAGE_NAME: $IMAGE_NAME"
echo "TAG: $TAG"
echo "DOCKERFILE_PATH: $DOCKERFILE_PATH"
echo "DOCKER_FILE_NAME: $DOCKER_FILE_NAME"

# Check if Dockerfile exists
if [ ! -f "$DOCKERFILE_PATH/$DOCKER_FILE_NAME" ]; then
    echo "Dockerfile not found at $DOCKERFILE_PATH"
    exit 1
fi

if [ "$DOCKERFILE_PATH" == *"docker/executableImage"* ]; then
    echo "Please use another script as this supports only build and dev images!"
    exit 1
fi


# Build the Docker image, but first cd to the directory where the Dockerfile is located and store the current directory
echo "Building the Docker image with the name $IMAGE_NAME and tag $TAG from the file $DOCKERFILE_PATH/$DOCKER_FILE_NAME..."
pushd $DOCKERFILE_PATH

# Build the Docker image
docker build -t $IMAGE_NAME:$TAG -f $DOCKER_FILE_NAME .
echo "Docker image $IMAGE_NAME:$TAG built successfully."

# Go back to the original directory
popd
