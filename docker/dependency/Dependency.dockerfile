# The dependency image contains a prebuilt sdk of all vcpkg depedencies used in the NebulaStream manifest
# This image is intended to be built as a multi-platform image where the ARCH is injected into the docker build.
# We do not use the builtin TARGETPLATFORM because it would not match the archtiecture names expected by VCPKG.
# We use x64 for amd64 and arm64 for aarch64. We always assume linux.

ARG TAG=latest

# fetch prebuilt deps in separate container to reduce overall image size
# c.f. https://docs.docker.com/build/building/multi-stage/
FROM alpine:latest AS prebuilt-deps-download
ARG ARCH
ARG SANITIZER="none"
ARG STDLIB=libcxx
ARG LLVM_VERSION=20
RUN apk update && apk add zstd
ADD https://github.com/nebulastream/clang-binaries/releases/download/vcustom-libcxx-mlir-${LLVM_VERSION}/nes-llvm-${LLVM_VERSION}-${ARCH}-${SANITIZER}-${STDLIB}.tar.zstd llvm.tar.zstd
RUN zstd --decompress llvm.tar.zstd   --stdout | tar -x
ADD https://github.com/nebulastream/clang-binaries/releases/download/vcustom-libcxx-mlir-${LLVM_VERSION}/nes-libcxx-${LLVM_VERSION}-${ARCH}-${SANITIZER}.tar.zstd libcxx.tar.zstd
RUN zstd --decompress libcxx.tar.zstd --stdout | tar -x



FROM nebulastream/nes-development-base:${TAG} AS vcpkg-build
COPY --from=prebuilt-deps-download /clang /clang
COPY --from=prebuilt-deps-download /libcxx /libcxx
ADD vcpkg /vcpkg_input
WORKDIR /vcpkg_input
ARG ARCH
ARG SANITIZER="none"
ARG STDLIB=libcxx
ARG VCPKG_DEPENDENCY_HASH
ENV CMAKE_PREFIX_PATH="/clang/:${CMAKE_PREFIX_PATH}"
ENV USE_CPP_STDLIB_LIBCXX_PATH=/libcxx
ENV VCPKG_DEPENDENCY_HASH=${VCPKG_DEPENDENCY_HASH}
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_SANITIZER=${SANITIZER}
ENV VCPKG_STDLIB=${STDLIB}
RUN git clone https://github.com/microsoft/vcpkg.git vcpkg_repository
RUN vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics
RUN vcpkg_repository/vcpkg install --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${SANITIZER}-${VCPKG_STDLIB}" --host-triplet="${ARCH}-linux-host"
RUN vcpkg_repository/vcpkg export --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${SANITIZER}-${VCPKG_STDLIB}" --host-triplet="${ARCH}-linux-host" --raw --output-dir / --output vcpkg
RUN chmod -R g=u,o=u /vcpkg


FROM nebulastream/nes-development-base:${TAG}
COPY --from=prebuilt-deps-download /clang /clang
COPY --from=prebuilt-deps-download /libcxx /libcxx
COPY --from=vcpkg-build /vcpkg /vcpkg
ARG ARCH
ARG SANITIZER="none"
ARG STDLIB=libcxx
ARG VCPKG_DEPENDENCY_HASH
ENV CMAKE_PREFIX_PATH="/clang/:${CMAKE_PREFIX_PATH}"
ENV VCPKG_DEPENDENCY_HASH=${VCPKG_DEPENDENCY_HASH}
ENV VCPKG_STDLIB=${STDLIB}
ENV VCPKG_SANITIZER=${SANITIZER}
ENV USE_CPP_STDLIB_LIBCXX_PATH=/libcxx
ENV NES_PREBUILT_VCPKG_ROOT=/vcpkg
