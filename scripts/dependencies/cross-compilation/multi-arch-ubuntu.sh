#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

set -ex

# create sysroot dir
sudo mkdir -p /user/local/toolchains/sysroots/ubuntu-arm && \

# install llvm build dependencies
sudo apt-get update -qq && sudo apt-get install -qq \
  build-essential \
  subversion \
  cmake \
  git \
  python3-dev \
  libncurses5-dev \
  libxml2-dev \
  libedit-dev \
  swig \
  doxygen \
  graphviz \
  xz-utils \
  ninja-build && \

# add sources for arm64 dependencies (from original amd64)
sudo cp /etc/apt/sources.list /etc/apt/sources.list.bkp && \
sudo sed -i -- 's|deb http|deb [arch=amd64] http|g' /etc/apt/sources.list && \
sudo cp /etc/apt/sources.list /etc/apt/sources.list.d/ports.list && \
sudo sed -i -- 's|amd64|arm64|g' /etc/apt/sources.list.d/ports.list && \

# packages from a different arch come from ports.ubuntu.com/ubuntu-ports
sudo sed -i -E -- 's|http://.*archive\.ubuntu\.com/ubuntu|http://ports.ubuntu.com/ubuntu-ports|g' /etc/apt/sources.list.d/ports.list && \
# actually ad repo to the system db
sudo dpkg --add-architecture arm64 && \
sudo apt-get update && \
# arm64 dependencies to build amd64-to-arm64 cross-compiling llvm
sudo apt-get install -qq libpython3-dev:arm64 libncurses5-dev:arm64 libxml2-dev:arm64 libedit-dev:arm64 && \

# faster clone from github mirror instead of official site (v10 is default in ubuntu LTS)
git clone --branch llvmorg-10.0.0 --single-branch https://github.com/llvm/llvm-project && \
mkdir -p llvm-project/build && cd llvm-project/build && \
cmake -G Ninja \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=/usr/local/llvm \
-DCMAKE_CROSSCOMPILING=ON \
-DLLVM_BUILD_DOCS=OFF \
-DLLVM_DEFAULT_TARGET_TRIPLE=aarch64-linux-gnu \
-DLLVM_TARGET_ARCH=AArch64 \
-DLLVM_TARGETS_TO_BUILD=AArch64 \
-DLLVM_ENABLE_PROJECTS="clang;compiler-rt;lld;polly;libcxx;libcxxabi;openmp" \
../llvm && \
ninja -j 2 clang && ninja -j 2 cxx && sudo ninja install && \
cd && rm -rf llvm-project && \

# LOCAL build needs a toolchain file
pushd
cat > toolchain-aarch64-llvm.cmake <<'EOT'
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_TARGET_ABI linux-gnu)
SET(CROSS_TARGET ${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_TARGET_ABI})

# directory of custom-compiled llvm with cross-comp support
set(CROSS_LLVM_PREFIX /user/local/llvm)

# specify the cross compiler
set(CMAKE_C_COMPILER ${CROSS_LLVM_PREFIX}/bin/clang)
set(CMAKE_CXX_COMPILER ${CROSS_LLVM_PREFIX}/bin/clang++)

# specify sysroot (our default: ubuntu-arm)
SET(CMAKE_SYSROOT /usr/local/toolchains/sysroots/ubuntu-arm)

# base version of gcc to include from in the system
SET(CROSS_GCC_BASEVER "9.3.0")

# Here be dragons! This is boilerplate code for wrangling CMake into working with a standalone LLVM
# installation and all of CMake's quirks. Side effect: Linker flags are no longer passed to ar (and
# neither should they be.) When editing, use plenty of fish to keep the dragon sated.
# More: https://mologie.github.io/blog/programming/2017/12/25/cross-compiling-cpp-with-cmake-llvm.html

SET(CROSS_LIBSTDCPP_INC_DIR "/usr/include/c++/${CROSS_GCC_BASEVER}")
SET(CROSS_LIBSTDCPPBITS_INC_DIR "${CROSS_LIBSTDCPP_INC_DIR}/${CROSS_TARGET}")
SET(CROSS_LIBGCC_DIR "/usr/lib/gcc/${CROSS_TARGET}/${CROSS_GCC_BASEVER}")
SET(CROSS_COMPILER_FLAGS "")
SET(CROSS_MACHINE_FLAGS "")
#SET(CROSS_MACHINE_FLAGS "-marm64 -march=armv8 -mfloat-abi=hard -mfpu=vfp")
SET(CROSS_LINKER_FLAGS "-fuse-ld=lld -stdlib=libstdc++ -L \"${CROSS_LIBGCC_DIR}\"")
SET(CMAKE_CROSSCOMPILING ON)
SET(CMAKE_AR "${CROSS_LLVM_PREFIX}/bin/llvm-ar" CACHE FILEPATH "" FORCE)
SET(CMAKE_NM "${CROSS_LLVM_PREFIX}/bin/llvm-nm" CACHE FILEPATH "" FORCE)
SET(CMAKE_RANLIB "${CROSS_LLVM_PREFIX}/bin/llvm-ranlib" CACHE FILEPATH "" FORCE)
SET(CMAKE_C_COMPILER_TARGET ${CROSS_TARGET})
SET(CMAKE_C_FLAGS "${CROSS_COMPILER_FLAGS} ${CROSS_MACHINE_FLAGS}" CACHE STRING "" FORCE)
SET(CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> qc <TARGET> <OBJECTS>")
SET(CMAKE_CXX_COMPILER_TARGET ${CROSS_TARGET})
SET(CMAKE_CXX_FLAGS "${CROSS_COMPILER_FLAGS} ${CROSS_MACHINE_FLAGS}" CACHE STRING "" FORCE)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -iwithsysroot \"${CROSS_LIBSTDCPP_INC_DIR}\"")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -iwithsysroot \"${CROSS_LIBSTDCPPBITS_INC_DIR}\"")
SET(CMAKE_CXX_ARCHIVE_CREATE "<CMAKE_AR> qc <TARGET> <OBJECTS>")
SET(CMAKE_EXE_LINKER_FLAGS ${CROSS_LINKER_FLAGS} CACHE STRING "" FORCE)
SET(CMAKE_MODULE_LINKER_FLAGS ${CROSS_LINKER_FLAGS} CACHE STRING "" FORCE)
SET(CMAKE_SHARED_LINKER_FLAGS ${CROSS_LINKER_FLAGS} CACHE STRING "" FORCE)
SET(CMAKE_STATIC_LINKER_FLAGS ${CROSS_LINKER_FLAGS} CACHE STRING "" FORCE)
SET(CMAKE_FIND_ROOT_PATH ${CROSS_LLVM_PREFIX})
SET(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
SET(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
SET(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
EOT
popd && \
sudo cp toolchain-aarch64-llvm.cmake /usr/local/toolchains/toolchain-aarch64-llvm.cmake && \

# cross-compile NES dependencies
cd && git clone --branch v1.28.1 https://github.com/grpc/grpc.git && \
  cd grpc && git submodule update --init --jobs 1 && mkdir -p build && cd build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=/usr/local/toolchains/toolchain-aarch64-llvm.cmake \
  -DCMAKE_INSTALL_PREFIX=/usr/local/toolchains/sysroots/ubuntu-arm/grpc_install \
  && make -j 2 install && cd ../.. && rm -rf grpc