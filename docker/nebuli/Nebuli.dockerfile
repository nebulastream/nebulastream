ARG TAG=latest
ARG BUILD_TYPE=RelWithDebInfo
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN cd /home/ubuntu/src \
    && cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 \
    && cmake --build build --target nes-nebuli -j \
    && mkdir /tmp/bin \
    && find build -name 'nes-nebuli' -type f -exec mv --target-directory=/tmp/bin {} +

# the binary is linked against libc++, thus we install it
FROM ubuntu:24.04 AS app
ENV LLVM_TOOLCHAIN_VERSION=19
RUN apt update -y && apt install curl gpg pipx -y
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-${LLVM_TOOLCHAIN_VERSION} libc++abi1-${LLVM_TOOLCHAIN_VERSION}

ENV PIPX_HOME=/opt/pipx \
    PIPX_BIN_DIR=/usr/local/bin \
    PATH=$PIPX_BIN_DIR:$PATH

RUN pipx ensurepath
RUN pipx install iree-base-compiler==3.3.0 && pipx inject iree-base-compiler onnx

COPY --from=build /tmp/bin /usr/bin
ENTRYPOINT ["nes-nebuli"]
