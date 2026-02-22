#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

RED='\033[0;31m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [-y|--yes] [-l|--local] [-r|--rootless] [--libstdcxx|--libcxx] [--address|--thread|--undefined] [--name]"
    echo "Options:"
    echo "  -y, --yes            Non-interactive mode (requires --libstdcxx or --libcxx)"
    echo "  -l, --local          Build all Docker images locally"
    echo "  -r, --rootless       Force rootless Docker mode"
    echo "  --libstdcxx          Use libstdcxx standard library"
    echo "  --libcxx             Use libcxx standard library"
    echo "  --address            Enable Address Sanitizer"
    echo "  --thread             Enable Thread Sanitizer"
    echo "  --undefined          Enable Undefined Behavior Sanitizer"
    echo "  --name               Add a name suffix to the tag (nes-development:local-<name>)"
    exit 1
}

# If set we built rebuilt all docker images locally
NON_INTERACTIVE=0
BUILD_LOCAL=0
FORCE_ROOTLESS=0
STDLIB=""
SANITIZER="none"
NAME_SUFFIX=""

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -y|--yes)
            NON_INTERACTIVE=1
            shift
            ;;
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
        --address)
            echo "Enabling Address Sanitizer"
            SANITIZER="address"
            shift
            ;;
        --thread)
            echo "Enabling Thread Sanitizer"
            SANITIZER="thread"
            shift
            ;;
        --undefined)
            echo "Enabling Undefined Behavior Sanitizer"
            SANITIZER="undefined"
            shift
            ;;
        --name)
            if [[ -n "$2" && ! "$2" =~ ^- ]]; then
                NAME_SUFFIX="$2"
                echo "Using name suffix: ${NAME_SUFFIX}"
                shift 2
            else
                echo "Error: --name requires a name argument"
                usage
            fi
            ;;
        -h|--help)
            usage
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
  if [ "$NON_INTERACTIVE" = 1 ]; then
    echo -e "${RED}Error: Non-interactive mode requires either --libstdcxx or --libcxx to be specified${NC}"
    usage
  fi
  echo "Please choose a standard library implementation:"
    echo "1. llvm libc++ "
    echo "2. gcc libstdc++ "
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

# If name suffix not set via command line, ask the user
if [[ -z "$NAME_SUFFIX" ]]; then
  if [ "$NON_INTERACTIVE" = 0 ]; then
    read -p "Do you want to specify a name for the Docker image? [y/N] " -r
    if [[ $REPLY =~ ^([yY][eE][sS]|[yY])$ ]]; then
      read -p "Enter the name suffix for the tax (image will be nes-development:local-<suffix>): " -r NAME_SUFFIX
      if [[ -z "$NAME_SUFFIX" ]]; then
        echo "No name provided, using default image name"
      else
        echo "Using name suffix: ${NAME_SUFFIX}"
      fi
    fi
  fi
fi

# Construct the final image name
if [[ -n "$NAME_SUFFIX" ]]; then
  FINAL_IMAGE_NAME="nebulastream/nes-development:local-${NAME_SUFFIX}"
else
  FINAL_IMAGE_NAME="nebulastream/nes-development:local"
fi

# Ask for confirmation of settings
echo # Move to a new line after input
echo "Build configuration:"
echo "- Standard library: ${STDLIB}"
echo "- Sanitizer: ${SANITIZER}"
echo "- Image name: ${FINAL_IMAGE_NAME}"
if [ "$NON_INTERACTIVE" = 0 ]; then
  read -p "Is this configuration correct? [Y/n] " -r
  echo # Move to a new line after input
  input=${REPLY:-Y}
  if [[ ! $input =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "Please re-run the script with the correct options."
    exit 1
  fi
else
  echo "Running in non-interactive mode, proceeding with configuration..."
fi

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
TAG=${HASH}-${ARCH}-${STDLIB}-${SANITIZER}

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


if [ $BUILD_LOCAL -eq 1 ]; then
  echo "Building local docker images using hash: ${HASH}."
  echo "This might take a while..."
  docker build -f docker/dependency/Base.dockerfile -t nebulastream/nes-development-base:local .

  if [[ -n "$R2_KEY" && -n "$R2_SECRET" && -n "$R2_ACCOUNT_ID" ]]; then
      echo "Secrets found. Building with R2 cache..."
      docker build -f docker/dependency/Dependency.dockerfile \
              --build-arg VCPKG_DEPENDENCY_HASH="${HASH}" \
              --secret id=R2_KEY,env=R2_KEY \
              --secret id=R2_SECRET,env=R2_SECRET \
              --secret id=R2_ACCOUNT_ID,env=R2_ACCOUNT_ID \
              --build-arg TAG=local \
              --build-arg STDLIB="${STDLIB}" \
              --build-arg ARCH="${ARCH}" \
              --build-arg SANITIZER="${SANITIZER}" \
              -t nebulastream/nes-development-dependency:local .
  else
      echo "Missing secrets. Building without R2 cache..."
      docker build -f docker/dependency/Dependency.dockerfile \
              --build-arg VCPKG_DEPENDENCY_HASH="${HASH}" \
              --build-arg TAG=local \
              --build-arg STDLIB="${STDLIB}" \
              --build-arg ARCH="${ARCH}" \
              --build-arg SANITIZER="${SANITIZER}" \
              -t nebulastream/nes-development-dependency:local .
  fi

  docker build -f docker/dependency/Development.dockerfile \
            --build-arg TAG=local \
            -t nebulastream/nes-development:default .

  docker build -f docker/dependency/DevelopmentLocal.dockerfile \
               -t ${FINAL_IMAGE_NAME} \
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
               -t ${FINAL_IMAGE_NAME} \
               --build-arg UID=${USE_UID} \
               --build-arg GID=${USE_GID} \
               --build-arg USERNAME=${USE_USERNAME} \
               --build-arg ROOTLESS=${USE_ROOTLESS} \
               --build-arg TAG=${TAG} .
fi

# Detect the docker socket path (works for both rootful and rootless Docker)
DOCKER_SOCKET=""

# Try to get docker socket path from context
DOCKER_CONTEXT_HOST=$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null || echo "")
if [ -n "$DOCKER_CONTEXT_HOST" ]; then
    # Extract path from unix:///path/to/docker.sock format
    DOCKER_SOCKET=$(echo "$DOCKER_CONTEXT_HOST" | sed 's|^unix://||')
fi

# Fallback: Check common locations
if [ ! -S "$DOCKER_SOCKET" ]; then
    # Try rootless location
    if [ -S "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock" ]; then
        DOCKER_SOCKET="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
    # Try rootful location
    elif [ -S "/var/run/docker.sock" ]; then
        DOCKER_SOCKET="/var/run/docker.sock"
    else
        DOCKER_SOCKET="/var/run/docker.sock"  # Default fallback
    fi
fi

echo ""
echo "========================================================================================"
echo "Docker Development Environment Setup Complete!"
echo "========================================================================================"
echo ""
echo "To enable Docker-in-Docker testing, mount the docker socket:"
echo ""
if [ -S "$DOCKER_SOCKET" ]; then
    echo "Detected docker socket at: $DOCKER_SOCKET"
    echo ""
    echo "Command line:"
    echo "  docker run -v $DOCKER_SOCKET:/var/run/docker.sock \\"
    echo "    -v \$(pwd):\$(pwd) -w \$(pwd) nebulastream/nes-development:local"
    echo ""
    echo "CLion Docker Toolchain (Settings → Docker → Container settings → Run options):"
    echo "  -v $DOCKER_SOCKET:/var/run/docker.sock"
else
    echo "Warning: Could not detect docker socket automatically."
    echo ""
    echo "Rootful Docker (default):"
    echo "  -v /var/run/docker.sock:/var/run/docker.sock"
    echo ""
    echo "Rootless Docker (typical):"
    echo "  -v \$XDG_RUNTIME_DIR/docker.sock:/var/run/docker.sock"
    echo "  or"
    echo "  -v /run/user/\$(id -u)/docker.sock:/var/run/docker.sock"
fi
echo ""
echo "Note: Docker CLI connects to host Docker daemon via socket. No isolation namespace."
echo "========================================================================================"
