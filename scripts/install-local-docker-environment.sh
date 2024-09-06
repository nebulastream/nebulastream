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
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -l|--local)
            BUILD_LOCAL=1
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

cd "$(git rev-parse --show-toplevel)"
HASH=$(docker/dependency/hash_dependencies.sh)

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
          --build-arg ARCH=${ARCH} \
          -t nebulastream/nes-development-dependency:local .

  docker build -f docker/dependency/Development.dockerfile \
            --build-arg TAG=local \
            -t nebulastream/nes-development:default .

  docker build -f docker/dependency/DevelopmentLocal.dockerfile \
               -t nebulastream/nes-development:local \
               --build-arg UID=$(id -u) \
               --build-arg GID=$(id -g) \
               --build-arg USERNAME=$(whoami) \
               --build-arg TAG=default .
else
  if ! docker manifest inspect nebulastream/nes-development:${HASH} > /dev/null 2>&1 ; then
   echo -e "${RED}Remote image development image for hash ${HASH} does not exist.
Either build locally with the -l option, or open a PR (draft) and let the CI build the development image${NC}"
   exit 1
  fi

  echo "Basing local development image on remote on nebulastream/nes-development:${HASH}"
  docker build -f docker/dependency/DevelopmentLocal.dockerfile \
               -t nebulastream/nes-development:local \
               --build-arg UID=$(id -u) \
               --build-arg GID=$(id -g) \
               --build-arg USERNAME=$(whoami) \
               --build-arg TAG=${HASH} .
fi
