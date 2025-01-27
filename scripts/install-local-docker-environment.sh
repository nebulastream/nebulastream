#!/bin/bash

set -e

RED='\033[0;31m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [-l|--local]"
    exit 1
}

# If set we built rebuilt all docker images locally
BUILD_LOCAL=0
FORCE_ROOTLESS=0
STDLIB=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -l|--local)
            BUILD_LOCAL=1
            shift
            ;;
        -r|--rootless)
            FORCE_ROOTLESS=1
            shift
            ;;
        --libstdcxx)
            echo "Set the standard library to libstdcxx"
            STDLIB=libstdcxx
            shift
            ;;
        --libcxx)
            echo "Set the standard library to libcxx"
            STDLIB=libcxx
            shift
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            shift
            ;;
    esac
done

# Check if the standard library is set, otherwise prompt the user
if [[ "$STDLIB" != "libcxx" && "$STDLIB" != "libstdcxx" ]]; then
  echo "Please choose a standard library implementation:"
    echo "1. libcxx (shipped with dependencies, likely to work, limited debug symbols in CLion)"
    echo "2. libstdcxx (not shipped with dependencies, takes local version, might lead to conflicts, full debug symbols in CLion)"
    read -p "Enter the number (1 or 2): " -r
    case $REPLY in
      1) STDLIB="libcxx" ;;
      2) STDLIB="libstdcxx" ;;
      *)
        echo "Invalid option. Please re-run the script and select 1 or 2."
        exit 1
        ;;
    esac
  fi

read -p "Building with ${STDLIB} standard library. Is this correct? [Y/n] " -r
echo # Move to a new line after input
input=${REPLY:-Y}
if [[ ! $input =~ ^([yY][eE][sS]|[yY])$ ]]; then
  echo "Please re-run the script with the correct option."
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"
HASH=$(docker/dependency/hash_dependencies.sh)
TAG=${HASH}
if [[ $STDLIB != 'libcxx' ]]; then
    TAG=${TAG}-libstdcxx
fi

# Docker on macOS appears to always enable the mapping from the container root user to the hosts current 
# user
if [[ $OSTYPE == 'darwin'* ]]; then
  FORCE_ROOTLESS=1
fi

# If Docker is running in rootless mode, the root user inside the container
# maps to the user running the rootless Docker daemon (likely the current user).
# Therefore, we can safely use the root user within the container.
# If rootless mode is not detected, we install the current user into the container.
# This assumes there is no custom user mapping between the host and container, which is reasonable.
USE_ROOTLESS=false
USE_UID=$(id -u)
USE_GID=$(id -g)
USE_USERNAME=$(whoami)
if docker info -f "{{println .SecurityOptions}}" | grep -q rootless || [ "$FORCE_ROOTLESS" = 1 ]; then
  echo "Detected docker rootless mode. Container internal user will be root"
  USE_ROOTLESS=true
  USE_UID=0
  USE_GID=0
  USE_USERNAME=root
fi

ARCH=$(uname -m)
if [ "$ARCH" == "x86_64" ]; then
   ARCH="x64"
elif [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
   ARCH="arm64"
else
  echo -e "${RED}Arch: $ARCH is not supported. Only x86_64 and aarch64|arm64 are handled. Arch is determined using 'uname -m'${NC}"
  exit 1
fi

if [ $BUILD_LOCAL -eq 1 ]; then
  echo "Building local docker images using hash: ${HASH}."
  echo "This might take a while..."
  docker build -f docker/dependency/Base.dockerfile -t nebulastream/nes-development-base:local .

  docker build -f docker/dependency/Dependency.dockerfile \
          --build-arg VCPKG_DEPENDENCY_HASH=${HASH} \
          --build-arg TAG=local \
          --build-arg STDLIB=${STDLIB} \
          --build-arg ARCH=${ARCH} \
          -t nebulastream/nes-development-dependency:local .

  docker build -f docker/dependency/Development.dockerfile \
            --build-arg TAG=local \
            -t nebulastream/nes-development:default .

  docker build -f docker/dependency/DevelopmentLocal.dockerfile \
               -t nebulastream/nes-development:local \
               --build-arg UID=${USE_UID} \
               --build-arg GID=${USE_GID} \
               --build-arg USERNAME=${USE_USERNAME} \
               --build-arg ROOTLESS=${USE_ROOTLESS} \
               --build-arg TAG=default .
else
  if ! docker manifest inspect nebulastream/nes-development:${TAG} > /dev/null 2>&1 ; then
   echo -e "${RED}Remote image development image for hash ${TAG} does not exist.
Either build locally with the -l option, or open a PR (draft) and let the CI build the development image${NC}"
   exit 1
  fi

  echo "Basing local development image on remote on nebulastream/nes-development:${TAG}"
  docker build -f docker/dependency/DevelopmentLocal.dockerfile \
               -t nebulastream/nes-development:local \
               --build-arg UID=${USE_UID} \
               --build-arg GID=${USE_GID} \
               --build-arg USERNAME=${USE_USERNAME} \
               --build-arg ROOTLESS=${USE_ROOTLESS} \
               --build-arg TAG=${TAG} .
fi
