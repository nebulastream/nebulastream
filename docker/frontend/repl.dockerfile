ARG TAG=latest
ARG BUILD_TYPE=RelWithDebInfo
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN --mount=type=cache,id=ccache,target=/ccache \
    export CCACHE_DIR=/ccache && \
    cd /home/ubuntu/src \
    && cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 \
    && cmake --build build --target nes-repl -j \
    && mkdir /tmp/bin \
    && find build -name 'nes-repl' -type f -exec mv --target-directory=/tmp/bin {} +

# the binary is linked against libc++, thus we install it
FROM ubuntu:24.04 AS app
ENV LLVM_TOOLCHAIN_VERSION=19
RUN apt-get update -y && apt-get install -y wget curl gpg
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt-get update -y \
    && apt-get install -y libc++1-${LLVM_TOOLCHAIN_VERSION} libc++abi1-${LLVM_TOOLCHAIN_VERSION}

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-$(dpkg --print-architecture) && \
    chmod +x /bin/grpc_health_probe

# Install IREE compiler tools for ML inference (ONNX → IREE compilation at runtime)
ARG IREE_COMPILER_VERSION=3.10.0
RUN apt-get update && apt-get install -y python3 python3-venv && \
    python3 -m venv /opt/iree && \
    /opt/iree/bin/pip install --no-cache-dir \
        iree-base-compiler==${IREE_COMPILER_VERSION} \
        iree-turbine \
        onnx && \
    ln -s /opt/iree/bin/iree-compile /usr/local/bin/iree-compile && \
    ln -s /opt/iree/bin/iree-import-onnx /usr/local/bin/iree-import-onnx && \
    iree-compile --version

COPY --from=build /tmp/bin /usr/bin
ENTRYPOINT ["nes-repl"]
