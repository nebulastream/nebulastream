# syntax=docker/dockerfile:1
# Lightweight runtime base image for running NebulaStream binaries.
# Contains only the runtime dependencies: libc++, grpc_health_probe, and basic utilities.
# The IREE compiler toolchain lives in RuntimeIree.dockerfile on top of this image, because only
# binaries that lower a query plan themselves can ever invoke it.
# This image is pre-built and pushed to the registry so that downstream images
# (worker, CLI, REPL, test containers) can skip network-heavy apt/wget steps at build time.
# Changes to this image are included in the development-image dependency hash.
FROM ubuntu:25.04

ARG LLVM_TOOLCHAIN_VERSION=19
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.40

# One RUN on purpose: curl/wget/gpg and the llvm apt source are only needed while building this
# layer, and Docker layers are additive, so deleting them in a later layer would not shrink the
# image. They have to go in the same layer that installed them.
RUN apt update -y && apt install -y --no-install-recommends curl wget gpg ca-certificates \
    && curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y --no-install-recommends libc++1-${LLVM_TOOLCHAIN_VERSION} libc++abi1-${LLVM_TOOLCHAIN_VERSION} \
    && wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-$(dpkg --print-architecture) \
    && chmod +x /bin/grpc_health_probe \
    && rm -f /etc/apt/sources.list.d/llvm-snapshot.list /etc/apt/keyrings/llvm-snapshot.gpg \
    && apt-get purge -y --auto-remove curl wget gpg \
    && apt clean && rm -rf /var/lib/apt/lists/*
