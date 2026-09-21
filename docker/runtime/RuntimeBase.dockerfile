# syntax=docker/dockerfile:1
# Lightweight runtime base image for running NebulaStream binaries.
# Contains only the runtime dependencies: libc++, grpc_health_probe, and basic utilities.
# This image is pre-built and pushed to the registry so that downstream images
# (worker, CLI, REPL, test containers) can skip network-heavy apt/wget steps at build time.
# Changes to this image are included in the development-image dependency hash.
FROM ubuntu:26.04

ARG LLVM_TOOLCHAIN_VERSION=22
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.52

RUN apt update -y && apt install curl wget gpg -y \
    && curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \

    && apt install -y libc++1 libc++abi1 libasound2t64 libatomic1 \
    && apt clean && rm -rf /var/lib/apt/lists/*

RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-$(dpkg --print-architecture) \
    && chmod +x /bin/grpc_health_probe

# Install the OpenVINO converter for ML inference model import at runtime.
# The converter has to match the OpenVINO runtime version NebulaStream links against, and that release has no wheels
# for the distribution's Python, so it runs on its own interpreter.
ARG OPENVINO_VERSION=2025.3.0
ARG OPENVINO_PYTHON_VERSION=3.13
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python
RUN apt-get update && apt-get install -y python3 python3-venv && \
    uv venv --python ${OPENVINO_PYTHON_VERSION} /opt/openvino && \
    uv pip install --no-cache --python /opt/openvino/bin/python openvino==${OPENVINO_VERSION} && \
    ln -s /opt/openvino/bin/ovc /usr/local/bin/ovc && \
    ovc --version && \
    apt clean && rm -rf /var/lib/apt/lists/*

# Runtime library for the PyPy-backed UDF bridge (nes-udf/bridge-pypy); see Development.dockerfile
# for the build-time pypy3-dev install.
RUN apt-get update && apt-get install -y pypy3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Downstream images run as this unprivileged user. uid/gid are pinned so volume ownership is
# stable across architectures; the numeric USER satisfies Kubernetes runAsNonRoot.
RUN groupadd --gid 10000 nes && useradd --uid 10000 --gid 10000 --create-home nes
USER 10000:10000
WORKDIR /home/nes
