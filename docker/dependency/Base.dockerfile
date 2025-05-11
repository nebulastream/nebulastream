# The Base Dockerfile installs and configures all relevant tooling to build the dependencies and NebulaStream.
# Currently our compiler toolchain is based on llvm-18 using libc++.
# Additionally we install a recent CMake version and the mold linker.
FROM ubuntu:24.04

ARG LLVM_TOOLCHAIN_VERSION=19
ARG MOLD_VERSION=2.37.1
ARG CMAKE_VERSION=3.31.6
ENV LLVM_TOOLCHAIN_VERSION=${LLVM_TOOLCHAIN_VERSION}
ENV CMAKE_VERSION=${CMAKE_VERSION}

RUN apt update -y && apt install \
    wget \
    zip \
    unzip \
    tar \
    curl \
    gpg \
    git \
    ca-certificates \
    linux-libc-dev \
    build-essential \
    g++-14 \
    make \
    libssl-dev \
    openssl \
    ccache \
    ninja-build \
    bison \
    pkg-config \
    pipx \
    -y

# install llvm based toolchain
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y && apt install clang-${LLVM_TOOLCHAIN_VERSION} libc++-${LLVM_TOOLCHAIN_VERSION}-dev libc++abi-${LLVM_TOOLCHAIN_VERSION}-dev libclang-rt-${LLVM_TOOLCHAIN_VERSION}-dev -y

# install recent version of the mold linker
RUN wget https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz \
    && tar -xf mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz \
    && cp -r mold-${MOLD_VERSION}-$(uname -m)-linux/* /usr \
    && rm -rf mold-${MOLD_VERSION}-$(uname -m)-linux mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz \
    && mold --version

# install recent version of cmake
RUN wget https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz \
    && tar -xf cmake-${CMAKE_VERSION}.tar.gz \
    && cd cmake-${CMAKE_VERSION} \
    && ./configure --parallel=$(nproc) --prefix=/usr \
    && make install -j $(nproc)\
    && cd .. \
    && rm -rf cmake-${CMAKE_VERSION}.tar.gz cmake-${CMAKE_VERSION} \
    && cmake --version

# default cmake generator is ninja
ENV CMAKE_GENERATOR=Ninja

# set default compiler to clang and make libc++ available via ldconfig
RUN ln -sf /usr/bin/clang-${LLVM_TOOLCHAIN_VERSION} /usr/bin/cc \
    && ln -sf /usr/bin/clang++-${LLVM_TOOLCHAIN_VERSION} /usr/bin/c++ \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-${LLVM_TOOLCHAIN_VERSION} 30\
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-${LLVM_TOOLCHAIN_VERSION} 30\
    && update-alternatives --auto cc \
    && update-alternatives --auto c++ \
    && update-alternatives --display cc \
    && update-alternatives --display c++ \
    && echo /usr/lib/llvm-${LLVM_TOOLCHAIN_VERSION}/lib > /etc/ld.so.conf.d/libcxx.conf \
    && ldconfig \
    && ls -l /usr/bin/cc /usr/bin/c++ \
    && cc --version \
    && c++ --version
