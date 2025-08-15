FROM ubuntu:24.04

# 1) Copy & run your install script
COPY scripts/install_sys_dependencies.sh /usr/local/bin/install_script.sh
RUN chmod +x /usr/local/bin/install_script.sh \
 && /usr/local/bin/install_script.sh

ARG LLVM_TOOLCHAIN_VERSION=19
RUN ln -sf /usr/bin/clang-${LLVM_TOOLCHAIN_VERSION}  /usr/bin/cc \
 && ln -sf /usr/bin/clang++-${LLVM_TOOLCHAIN_VERSION} /usr/bin/c++ \
 && update-alternatives --install /usr/bin/cc  cc  /usr/bin/clang-${LLVM_TOOLCHAIN_VERSION}  30 \
 && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-${LLVM_TOOLCHAIN_VERSION} 30 \
 && update-alternatives --auto cc  && update-alternatives --auto c++ \
 && echo /usr/lib/llvm-${LLVM_TOOLCHAIN_VERSION}/lib > /etc/ld.so.conf.d/libcxx.conf \
 && ldconfig

# 2) Install Ninja in-container
RUN apt update -y \
 && apt install -y ninja-build

# 3) Default CMake generator and llvm toolchain version since it's only set in non-docker builds per default
ENV CMAKE_GENERATOR=Ninja
ENV LLVM_TOOLCHAIN_VERSION=19
