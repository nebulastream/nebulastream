FROM ghcr.io/actions/actions-runner:latest

USER root

RUN apt update -y && apt install cmake curl -y
ARG LLVM_VERSION=18
ENV LLVM_VERSION=${LLVM_VERSION}
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
	&& chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
	&& echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release \
	&& echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
	&& echo   "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
	&& cat /etc/apt/sources.list.d/llvm-snapshot.list


RUN apt update -y && apt install clang-${LLVM_VERSION} libc++-${LLVM_VERSION}-dev libc++abi-${LLVM_VERSION}-dev libclang-rt-${LLVM_VERSION}-dev cmake make linux-libc-dev ninja-build git zip unzip tar curl pkg-config mold -y

RUN ln -sf /usr/bin/clang-${LLVM_VERSION} /usr/bin/cc \
  && ln -sf /usr/bin/clang++-${LLVM_VERSION} /usr/bin/c++ \
  && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-${LLVM_VERSION} 10\
  && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-${LLVM_VERSION} 10\
  && update-alternatives --auto cc \
  && update-alternatives --auto c++ \
  && update-alternatives --display cc \
  && update-alternatives --display c++ \
  && echo /usr/lib/llvm-${LLVM_VERSION}/lib > /etc/ld.so.conf.d/libcxx.conf \
  && ldconfig \
  && ls -l /usr/bin/cc /usr/bin/c++ \
  && cc --version \
  && c++ --version

ARG VCPKG_CACHE_SERVICE=vcpkg-cache
ENV VCPKG_BINARY_SOURCES=http,http://${VCPKG_CACHE}:15151/{name}/{version}/{sha},readwrite

USER runner

ADD vcpkg /home/runner/vcpkg
ARG BUILD_TYPE=nes
RUN  git clone https://github.com/microsoft/vcpkg vcpkg_build \
  && vcpkg_build/bootstrap-vcpkg.sh --disableMetrics \
  && vcpkg_build/vcpkg install --triplet="x64-linux-$BUILD_TYPE" --host-triplet="x64-linux-nes" --overlay-triplets=/home/runner/vcpkg/custom-triplets --overlay-ports=/home/runner/vcpkg/vcpkg-registry/ports/ \
  && mkdir -p vcpkg/scripts/buildsystems \
  && mv vcpkg_build/vcpkg_installed vcpkg/installed \
  && cp vcpkg_build/scripts/buildsystems/vcpkg.cmake vcpkg/scripts/buildsystems/ \
  && rm -rf vcpkg_build \
  && touch vcpkg/.vcpkg-root
