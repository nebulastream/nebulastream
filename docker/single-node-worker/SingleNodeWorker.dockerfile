ARG TAG=latest
ARG BUILD_TYPE=RelWithDebInfo
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN cd /home/ubuntu/src \
    && cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 \
    && cmake --build build --target single-node -j \
    && mkdir /tmp/lib /tmp/bin \
    && find build -name '*.so' -exec mv -t /tmp/lib {} + \
    && find build -name 'single-node' -exec mv -t /tmp/bin {} +


FROM ubuntu:24.04 AS app
ENV LLVM_VERSION=18
RUN apt update -y && apt install curl gpg -y
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y && apt install libc++1-${LLVM_VERSION} libc++abi1-${LLVM_VERSION} -y

COPY --from=build /tmp/lib /usr/lib
COPY --from=build /tmp/bin /usr/bin
ENTRYPOINT ["single-node"]
