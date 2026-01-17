# The dependency image contains a prebuilt sdk of all vcpkg depedencies used in the NebulaStream manifest
# This image is intended to be built as a multi-platform image where the ARCH is injected into the docker build.
# We do not use the builtin TARGETPLATFORM because it would not match the archtiecture names expected by VCPKG.
# We use x64 for amd64 and arm64 for aarch64. We always assume linux.

ARG TAG=latest
# Download and extract mlir in a separate build container to reduce the overall image size by only
# copying the required files into the final dependency container
FROM alpine:latest AS llvm-download
# Which architecture to build the dependencies for. Either x64 or arm64, matches the names in our vcpkg toolchains
ARG ARCH
ARG SANITIZER="none"
ARG STDLIB=libcxx
ARG LLVM_VERSION=21-with-fix-173075
RUN apk update && apk add zstd
ADD https://github.com/nebulastream/clang-binaries/releases/download/vmlir-${LLVM_VERSION}/nes-llvm-${LLVM_VERSION}-${ARCH}-${SANITIZER}-${STDLIB}.tar.zstd llvm.tar.zstd

RUN zstd --decompress llvm.tar.zstd --stdout | tar -x

# syntax=docker/dockerfile:1
FROM nebulastream/nes-development-base:${TAG} as dep-build
ARG STDLIB=libcxx
ARG ARCH
ARG SANITIZER="none"

# Install AWS CLI
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        URL="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl "$URL" -o "awscliv2.zip" && \
    unzip -q awscliv2.zip && \
    ./aws/install -i /usr/local/aws-cli -b /usr/local/bin && \
    rm -rf awscliv2.zip aws

COPY --from=llvm-download /clang /clang
ENV CMAKE_PREFIX_PATH="/clang/:${CMAKE_PREFIX_PATH}"

ADD vcpkg /vcpkg_input
ENV VCPKG_FORCE_SYSTEM_BINARIES=1

RUN --mount=type=secret,id=R2_KEY \
    --mount=type=secret,id=R2_SECRET \
    --mount=type=secret,id=R2_ACCOUNT_ID \
    bash -c ' \
    set -e; \
    if [ -f /run/secrets/R2_ACCOUNT_ID ]; then \
        echo "R2 Secrets found. Configuring cloud binary cache..."; \
        export R2_ID=$(cat /run/secrets/R2_ACCOUNT_ID); \
        export AWS_ACCESS_KEY_ID=$(cat /run/secrets/R2_KEY); \
        export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/R2_SECRET); \
        export AWS_ENDPOINT_URL_S3="https://${R2_ID}.r2.cloudflarestorage.com"; \
        export AWS_REGION="auto"; \
        export VCPKG_BINARY_SOURCES="clear;x-aws,s3://my-vcpkg-cache/,readwrite"; \
    else \
        echo "No secrets found. Building without cloud cache..."; \
        export VCPKG_BINARY_SOURCES="clear;default,readwrite"; \
    fi; \
    \
    if [ "$STDLIB" = "libcxx" ]; then VCPKG_STDLIB="libcxx"; else VCPKG_STDLIB="local"; fi; \
    \
    cd /vcpkg_input; \
    git clone https://github.com/microsoft/vcpkg.git vcpkg_repository; \
    ./vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics; \
    \
    ./vcpkg_repository/vcpkg install \
        --overlay-triplets=custom-triplets \
        --overlay-ports=vcpkg-registry/ports \
        --triplet="${ARCH}-linux-${SANITIZER}-${VCPKG_STDLIB}" \
        --host-triplet="${ARCH}-linux-none-${VCPKG_STDLIB}"'

RUN bash -c ' \
    set -e; \
    if [ "$STDLIB" = "libcxx" ]; then VCPKG_STDLIB="libcxx"; else VCPKG_STDLIB="local"; fi; \
    \
    cd /vcpkg_input; \
    ./vcpkg_repository/vcpkg export \
        --overlay-triplets=custom-triplets \
        --overlay-ports=vcpkg-registry/ports \
        --triplet="${ARCH}-linux-${SANITIZER}-${VCPKG_STDLIB}" \
        --raw --output-dir=/ --output=vcpkg; \
    \
    chmod -R g=u,o=u /vcpkg'

FROM nebulastream/nes-development-base:${TAG}

COPY --from=llvm-download /clang /clang
COPY --from=dep-build /vcpkg /vcpkg
# This hash is used to determine if a development/dependency image is compatible with the current checked out branch

ENV CMAKE_PREFIX_PATH="/clang/:${CMAKE_PREFIX_PATH}"
ARG STDLIB=libcxx
ARG VCPKG_DEPENDENCY_HASH
ARG SANITIZER="none"
ENV VCPKG_DEPENDENCY_HASH=${VCPKG_DEPENDENCY_HASH}
ENV VCPKG_STDLIB=${STDLIB}
ENV VCPKG_SANITIZER=${SANITIZER}
ENV NES_PREBUILT_VCPKG_ROOT=/vcpkg
