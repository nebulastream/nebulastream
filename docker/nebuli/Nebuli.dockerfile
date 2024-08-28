ARG TAG=latest
FROM nebulastream/nes-development:latest AS build

USER root
ADD . /home/ubuntu/src
RUN cd /home/ubuntu/src \
    && cmake -B build -S . \
    && cmake --build build --target nebuli -j \
    && cd build \
    && cpack \
    && mv *.deb /nebuli.deb


FROM ubuntu:24.04 AS app
ENV LLVM_VERSION=18
RUN apt update -y && apt install curl gpg -y
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y && apt install -y clang-${LLVM_VERSION} libc++-${}

RUN ln -sf /usr/bin/clang-${LLVM_VERSION} /usr/bin/cc \
    && ln -sf /usr/bin/clang++-${LLVM_VERSION} /usr/bin/c++ \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-${LLVM_VERSION} 30\
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-${LLVM_VERSION} 30\
    && update-alternatives --auto cc \
    && update-alternatives --auto c++ \
    && update-alternatives --display cc \
    && update-alternatives --display c++

COPY --from=build /nebuli.deb /nebuli.deb
RUN apt install /nebuli.deb -y
ENTRYPOINT ["nebuli"]
