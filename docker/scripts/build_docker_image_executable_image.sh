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
NEBULA_STREAM_PATH=$(cd .. && pwd) # We expect the script to be executed from the docker directory
DOCKERFILE_PATH_DEFAULT="./executableImage"
DOCKERFILE_PATH_BUILD_IMAGE_DEFAULT="./buildImage"
DOCKER_FILE_NAME_DEFAULT="Dockerfile-NES-Executable-Multi-Arch"
NUM_THREADS_DEFAULT=$(nproc)

# Assign arguments with default fallbacks
BUILD_DIR="${1:-$BUILD_DIR_DEFAULT}"
CCACHE_DIR="${2:-$CCACHE_DIR_DEFAULT}"
NEBULA_STREAM_PATH="${3:-$NEBULA_STREAM_PATH}"
IMAGE_NAME="${4:-$IMAGE_NAME_DEFAULT}"
TAG="${5:-$TAG_DEFAULT}"
ARCH="${6:-$ARCH_DEFAULT}"
PACKAGE_NAME="${7:-$PACKAGE_NAME_DEFAULT}"
PLATFORM_NAME="${8:-$PLATFORM_NAME_DEFAULT}"
DOCKERFILE_PATH="${9:-$DOCKERFILE_PATH_DEFAULT}"
DOCKER_FILE_NAME="${10:-$DOCKER_FILE_NAME_DEFAULT}"
NUM_THREADS="${11:-$NUM_THREADS_DEFAULT}"
DOCKERFILE_PATH_BUILD_IMAGE="${12:-$DOCKERFILE_PATH_BUILD_IMAGE_DEFAULT}"

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

# Ask the user if they want to proceed with building the Docker image
echo "Do you want to build the Docker image with the name $IMAGE_NAME and tag $TAG from the file $DOCKERFILE_PATH?
We first have to compile NebulaStream to create an executable image. We will use $NUM_THREADS threads for the compilation.
Would you like to continue as this might take some time? (y/n)"
read -r user_confirmation

if [[ $user_confirmation == "y" || $user_confirmation == "Y" ]]; then
    # Build the Docker Image for Prepare Package
    docker_image_name_creating_package="nes_package"
    docker_runner_creating_package="nes_package_running_${ARCH}"

    # Build the Docker image, but first cd to the directory where the Dockerfile is located and store the current directory
    cur_dir=$(pwd)
    cd "$DOCKERFILE_PATH_BUILD_IMAGE"
    docker build  -t "$docker_image_name_creating_package" -f "Dockerfile-NES-Build" .
    cd $cur_dir

    # Run the Docker image to create the package
    echo "docker run --name $docker_runner_creating_package -v $BUILD_DIR:/build_dir -v $CCACHE_DIR:/cache_dir -v $NEBULA_STREAM_PATH:/nebulastream --privileged --cap-add SYS_NICE --entrypoint /nebulastream/docker/buildImage/entrypoint-prepare-nes-package.sh "$docker_image_name_creating_package""
    docker run --name $docker_runner_creating_package -v $BUILD_DIR:/build_dir -v $CCACHE_DIR:/cache_dir -v $NEBULA_STREAM_PATH:/nebulastream --privileged --cap-add SYS_NICE --entrypoint /nebulastream/docker/buildImage/entrypoint-prepare-nes-package.sh "$docker_image_name_creating_package" $NUM_THREADS
    docker rm $docker_runner_creating_package

    # We have to rename the debian packages to either nes-amd64.deb or nes-arm64.deb, before we can build the docker image, as the dockerfile expects the names amd64 or arm64
    cur_dir=$(pwd)
    cd $DOCKERFILE_PATH
    cp $BUILD_DIR/*.deb nes-$PACKAGE_NAME.deb
    docker buildx build . -f $DOCKER_FILE_NAME --platform=$PLATFORM_NAME --tag nebulastream/nes-public-executable-image:nightly-$PACKAGE_NAME
    cd $cur_dir
else
    echo "Docker image build cancelled."
    exit 0
fi

#rm -rf $BUILD_DIR
#if [ "$CCACHE_DIR" != "NONE" ]; then
#    rm -rf $CCACHE_DIR
#fi