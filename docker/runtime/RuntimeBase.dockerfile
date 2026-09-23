# syntax=docker/dockerfile:1
# Lightweight runtime base image for running NebulaStream binaries.
# Contains only the runtime dependencies: libc++, grpc_health_probe, and basic utilities.
# This image is pre-built and pushed to the registry so that downstream images
# (worker, CLI, REPL, test containers) can skip network-heavy apt/wget steps at build time.
# Changes to this image are included in the development-image dependency hash.
FROM ubuntu:26.04

ARG LLVM_TOOLCHAIN_VERSION=22
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.40

RUN apt update -y && apt install curl wget gpg -y \
    && curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1 libc++abi1 \
    && apt clean && rm -rf /var/lib/apt/lists/*

RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-$(dpkg --print-architecture) \
    && chmod +x /bin/grpc_health_probe

# Install the OpenVINO converter for ML inference model import at runtime.
# The converter has to match the OpenVINO version we link against (2025.3.0), whose wheels only exist up to
# Python 3.13, but Ubuntu 26.04 ships Python 3.14. We therefore create the venv on a standalone Python 3.13 via uv.
# uv is only needed at build time and is removed again, the Python interpreter stays in /opt/python.
ARG OPENVINO_VERSION=2025.3.0
ARG OPENVINO_PYTHON_VERSION=3.13
ARG UV_VERSION=0.12.17
RUN apt-get update && apt-get install -y python3 python3-venv && \
    python3 -m venv /opt/uv && \
    /opt/uv/bin/pip install --no-cache-dir uv==${UV_VERSION} && \
    UV_PYTHON_INSTALL_DIR=/opt/python /opt/uv/bin/uv venv --python ${OPENVINO_PYTHON_VERSION} /opt/openvino && \
    /opt/uv/bin/uv pip install --python /opt/openvino/bin/python --no-cache openvino==${OPENVINO_VERSION} && \
    rm -rf /opt/uv && \
    ln -s /opt/openvino/bin/ovc /usr/local/bin/ovc && \
    ovc --version && \
    apt clean && rm -rf /var/lib/apt/lists/*
