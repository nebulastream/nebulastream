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
ARG ENABLE_SANITIZER=None
ARG STDLIB=libcxx
RUN apk update && apk add wget tar
ADD https://github.com/nebulastream/clang-binaries/releases/download/vmlir-sanitized/nes-clang-18-${STDLIB}-${ARCH}-${ENABLE_SANITIZER}.tar.gz .
RUN tar -C / -xzvf nes-clang-18-${STDLIB}-${ARCH}-${ENABLE_SANITIZER}.tar.gz && ls -l /


FROM nebulastream/nes-development-base:${TAG}
ARG STDLIB=libcxx

COPY --from=llvm-download /clang /clang
ENV CMAKE_PREFIX_PATH="/clang/:${CMAKE_PREFIX_PATH}"

ADD vcpkg /vcpkg_input
ARG ENABLE_SANITIZER
ARG ARCH
ENV VCPKG_FORCE_SYSTEM_BINARIES=1

RUN \
    if [ "$STDLIB" = "libcxx" ]; then \
      export VCPKG_STDLIB="libcxx"; \
    else \
      export VCPKG_STDLIB="local"; \
    fi; \
    if [ "$ENABLE_SANITIZER" = "Address" ]; then \
      export VCPKG_VARIANT="asan"; \
    elif [ "$ENABLE_SANITIZER" = "Undefined" ]; then \
      export VCPKG_VARIANT="ubsan"; \
    elif [ "$ENABLE_SANITIZER" = "Thread" ]; then \
      export VCPKG_VARIANT="tsan"; \
    else \
      export VCPKG_VARIANT="none"; \
    fi; \
    cd /vcpkg_input \
    && git clone https://github.com/microsoft/vcpkg.git vcpkg_repository \
    && vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics \
    && vcpkg_repository/vcpkg install --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${VCPKG_VARIANT}-${VCPKG_STDLIB}" --host-triplet="${ARCH}-linux-${VCPKG_VARIANT}-${VCPKG_STDLIB}" \
    && vcpkg_repository/vcpkg export --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${VCPKG_VARIANT}-${VCPKG_STDLIB}" --host-triplet="${ARCH}-linux-${VCPKG_VARIANT}-${VCPKG_STDLIB}" --raw --output-dir / --output vcpkg \
    && rm -rf /vcpkg_input \
    && chmod -R g=u,o=u /vcpkg

# This hash is used to determine if a development/dependency image is compatible with the current checked out branch
ARG VCPKG_DEPENDENCY_HASH
ENV VCPKG_DEPENDENCY_HASH=${VCPKG_DEPENDENCY_HASH}
ENV VCPKG_STDLIB=${STDLIB}
ENV VCPKG_VARIANT=${ENABLE_SANITIZER}
ENV NES_PREBUILT_VCPKG_ROOT=/vcpkg
