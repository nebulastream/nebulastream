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
RUN apk update && apk add wget 7zip
ADD https://github.com/nebulastream/clang-binaries/releases/download/vllvm-18-libc%2B%2B-mlir-only/nes-clang-18-ubuntu-22.04-libc++-${ARCH}.7z nes-clang
RUN 7z x nes-clang && rm nes-clang

FROM nebulastream/nes-development-base:${TAG}
ADD vcpkg /vcpkg_input
# Which vcpkg variant, e.g. with enabled sanitizers or the default `nes` variant.
ARG VARIANT=nes
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
# Which architecture to build the dependencies for. Either x64 or arm64, matches the names in our vcpkg toolchains
ARG ARCH
RUN cd /vcpkg_input \
    && git clone https://github.com/microsoft/vcpkg.git vcpkg_repository \
    && vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics \
    && vcpkg_repository/vcpkg install --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${VARIANT}" --host-triplet="${ARCH}-linux-${VARIANT}" \
    && vcpkg_repository/vcpkg export --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${VARIANT}" --host-triplet="${ARCH}-linux-${VARIANT}" --raw --output-dir / --output vcpkg \
    && cp vcpkg_repository/scripts/vcpkgTools.xml /vcpkg/scripts/ \
    && rm -rf /vcpkg_input \
    && chmod -R g=u,o=u /vcpkg

COPY --from=llvm-download /clang /clang
ENV CMAKE_PREFIX_PATH="/clang/:${CMAKE_PREFIX_PATH}"

# This hash is used to determine if a development/dependency image is compatible with the current checked out branch
ARG VCPKG_DEPENDENCY_HASH
ENV VCPKG_DEPENDENCY_HASH=${VCPKG_DEPENDENCY_HASH}
ENV NES_PREBUILT_VCPKG_ROOT=/vcpkg
