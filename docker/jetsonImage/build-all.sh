#!/bin/bash

# This script builds NebulaStream Docker image which can run an OpenCL kernel on the GPU of an Nvidia Jetson via POCL.
# It should be executed on an Nvidia Jetson device because it needs the Jetson-specific Nvidia Docker runtime.
# It assumes that is is run from a checked out NebulaStream repository

# Build NebulaStream build image with local LLVM 12
docker build -t nes-build_ubuntu-20.04_llvm-12 -f docker/jetsonImage/Dockerfile-NES-Build-ubuntu-20.04_LLVM-12 docker/jetsonImage

# Build NebulaStream Debian package
mkdir -p /tmp/build-tmp/{nes_build_dir,nes_cache_dir}
docker run --rm -v /tmp/build-tmp/nes_cache_dir:/cache_dir -v /tmp/build-tmp/nes_build_dir:/build_dir -v $(pwd):/nebulastream --privileged --cap-add SYS_NICE --entrypoint /nebulastream/docker/jetsonImage/entrypoint-prepare-nes-package.sh nes-build_ubuntu-20.04_llvm-12

# Copy NebulaStream Debian package
cp /tmp/build-tmp/nes_build_dir/NebulaStream-0.6.19-1-Linux.aarch64.deb nes-arm64.deb

# Checkout POCL
git clone https://github.com/pocl/pocl.git /tmp/build-tmp/pocl
(cd /tmp/build-tmp/pocl && git checkout release_1_7)

# Build POCL build image with LLVM 12
docker build -t pocl-build_llvm-12 -f docker/jetsonImage/Dockerfile-POCL-Build_LLVM-12 docker/jetsonImage

# Build POCL
docker run --rm -it -v /tmp/build-tmp/pocl_build_dir:/build_dir -v /tmp/build-tmp/pocl:/pocl pocl-build_llvm-12 /entrypoint-build-pocl.sh

# Copy POCL Debian packages
cp /tmp/build-tmp/pocl_build_dir/*deb .

# Build NebulaStream Jetson image
docker build -t nebulastream/nes-elegant-image:0.6.19-jetson -f docker/jetsonImage/Dockerfile_Jetson-image .

# Build and push to Docker Hub
docker push nebulastream/nes-elegant-image:0.6.19-jetson

