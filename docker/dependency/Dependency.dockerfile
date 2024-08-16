# The dependency image contains a prebuilt sdk of all vcpkg depedencies used in the NebulaStream manifest
# This image is intended to be built as a multi-platform image where the ARCH is injected into the docker build.
# We do not use the builtin TARGETPLATFORM because it would not match the archtiecture names expected by VCPKG.
# We use x64 for amd64 and arm64 for aarch64. We always assume linux.
ARG TAG=latest
FROM nebulastream/nes-development-base:${TAG}
ADD vcpkg /vcpkg_input
ARG ARCH
ARG VARIANT=nes
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
RUN cd /vcpkg_input \
    && git clone https://github.com/microsoft/vcpkg.git vcpkg_repository \
    && vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics \
    && vcpkg_repository/vcpkg install --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${VARIANT}" --host-triplet="${ARCH}-linux-${VARIANT}" --x-feature=mlir \
    && vcpkg_repository/vcpkg export --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="${ARCH}-linux-${VARIANT}" --host-triplet="${ARCH}-linux-${VARIANT}" --raw --output-dir / --output vcpkg \
    && cp vcpkg_repository/scripts/vcpkgTools.xml /vcpkg/scripts/ \
    && rm -rf /vcpkg_input \
    && chmod -R g=u,o=u /vcpkg
ENV NES_PREBUILT_VCPKG_ROOT=/vcpkg
