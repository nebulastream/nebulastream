#!/bin/bash


# To build an executable image for the checked out branch, run the script with the following command:
# bash scripts/build_docker_image_non_executable_image.sh


# Default values for arguments
IMAGE_NAME_DEFAULT="nes-public-executable-image"
TAG_DEFAULT="latest"
ARCH_DEFAULT="X64"
PACKAGE_NAME_DEFAULT="amd64"
PLATFORM_NAME_DEFAULT="linux/amd64"
BUILD_DIR_DEFAULT="./build_dir"
CCACHE_DIR_DEFAULT="none"
NEBULA_STREAM_PATH=$(realpath ..) # We expect the script to be executed from the docker directory
DOCKERFILE_PATH_DEFAULT="./executableImage"
DOCKERFILE_PATH_BUILD_IMAGE_DEFAULT="./buildImage"
DOCKER_FILE_NAME_DEFAULT="Dockerfile-NES-Executable-Multi-Arch"
NUM_THREADS_DEFAULT=$(nproc)

# Assign arguments with default fallbacks
TAG="${1:-$TAG_DEFAULT}"
ARCH="${2:-$ARCH_DEFAULT}"
PACKAGE_NAME="${3:-$PACKAGE_NAME_DEFAULT}"
PLATFORM_NAME="${4:-$PLATFORM_NAME_DEFAULT}"
IMAGE_NAME="${5:-$IMAGE_NAME_DEFAULT}"
BUILD_DIR="${6:-$BUILD_DIR_DEFAULT}"
CCACHE_DIR="${7:-$CCACHE_DIR_DEFAULT}"
NEBULA_STREAM_PATH="${8:-$NEBULA_STREAM_PATH}"
DOCKERFILE_PATH="${9:-$DOCKERFILE_PATH_DEFAULT}"
DOCKER_FILE_NAME="${10:-$DOCKER_FILE_NAME_DEFAULT}"
NUM_THREADS="${11:-$NUM_THREADS_DEFAULT}"
DOCKERFILE_PATH_BUILD_IMAGE="${12:-$DOCKERFILE_PATH_BUILD_IMAGE_DEFAULT}"

# Printing out all the arguments
echo "TAG: $TAG"
echo "ARCH: $ARCH"
echo "PACKAGE_NAME: $PACKAGE_NAME"
echo "PLATFORM_NAME: $PLATFORM_NAME"
echo "BUILD_DIR: $BUILD_DIR"
echo "CCACHE_DIR: $CCACHE_DIR"
echo "NEBULA_STREAM_PATH: $NEBULA_STREAM_PATH"
echo "IMAGE_NAME: $IMAGE_NAME"
echo "DOCKERFILE_PATH: $DOCKERFILE_PATH"
echo "DOCKER_FILE_NAME: $DOCKER_FILE_NAME"
echo "NUM_THREADS: $NUM_THREADS"
echo "DOCKERFILE_PATH_BUILD_IMAGE: $DOCKERFILE_PATH_BUILD_IMAGE"

# Check if Dockerfile exists
if [ ! -f "$DOCKERFILE_PATH/$DOCKER_FILE_NAME" ]; then
    echo "Dockerfile not found at $DOCKERFILE_PATH"
    exit 1
fi

# Check if NebulaStream path exists and a CMakeLists.txt file is present
if [ ! -f "$NEBULA_STREAM_PATH/CMakeLists.txt" ]; then
    echo "Can not find a CMakeLists.txt at $NEBULA_STREAM_PATH..."
    exit 1
fi

# Creating all necessary directories, if they do not exist
mkdir -p $BUILD_DIR
if [ "$CCACHE_DIR" != "NONE" ]; then
    mkdir -p $CCACHE_DIR
fi

# Getting for all paths the absolute path
BUILD_DIR=$(realpath $BUILD_DIR)
CCACHE_DIR=$(realpath $CCACHE_DIR)
NEBULA_STREAM_PATH=$(realpath $NEBULA_STREAM_PATH)
DOCKERFILE_PATH=$(realpath $DOCKERFILE_PATH)
DOCKERFILE_PATH_BUILD_IMAGE=$(realpath $DOCKERFILE_PATH_BUILD_IMAGE)

echo "Building a docker image with the name $IMAGE_NAME and tag $TAG from the file $DOCKERFILE_PATH.
We will first compile NebulaStream to create an executable image. We will use $NUM_THREADS threads for the compilation."

# Build the Docker Image for Prepare Package
docker_image_name_creating_package="nes_package"
docker_runner_creating_package="nes_package_running_${ARCH}_$(LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 6)"

# Build the Docker image, but first cd to the directory where the Dockerfile is located and store the current directory
pushd "$DOCKERFILE_PATH_BUILD_IMAGE"
docker build  -t "$docker_image_name_creating_package" -f "Dockerfile-NES-Build" .
popd

# Run the Docker image to create the package
docker run  --rm --name $docker_runner_creating_package -v $BUILD_DIR:/build_dir -v $CCACHE_DIR:/cache_dir -v $NEBULA_STREAM_PATH:/nebulastream --privileged --cap-add SYS_NICE --entrypoint /nebulastream/docker/buildImage/entrypoint-prepare-nes-package.sh "$docker_image_name_creating_package" $NUM_THREADS
echo "NebulaStream has been compiled and the package has been created under $BUILD_DIR"

# We have to rename the debian packages to either nes-amd64.deb or nes-arm64.deb, before we can build the docker image, as the dockerfile expects the names amd64 or arm64
pushd $DOCKERFILE_PATH
cp $BUILD_DIR/*.deb $PACKAGE_NAME.deb
echo "The debian package has been renamed to $PACKAGE_NAME.deb and copied from $BUILD_DIR to $(pwd)"
docker buildx build . -f $DOCKER_FILE_NAME --platform=$PLATFORM_NAME --tag nebulastream/$IMAGE_NAME:$TAG
popd

rm -rf $BUILD_DIR