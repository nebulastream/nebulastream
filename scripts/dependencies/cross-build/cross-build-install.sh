#!/bin/bash

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

# install llvm build dependencies
sudo apt-get update -qq && sudo DEBIAN_FRONTEND="noninteractive" apt-get install -qq \
  build-essential \
  ca-certificates \
  crossbuild-essential-arm64 \
  libstdc++6-arm64-cross \
  cmake \
  git \
  python3-dev \
  python-dev \
  libncurses5-dev \
  libxml2-dev \
  libedit-dev \
  libmbedtls-dev \
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
sudo sed -i -E -- 's|http://.*security\.ubuntu\.com/ubuntu|http://ports.ubuntu.com/ubuntu-ports|g' /etc/apt/sources.list.d/ports.list && \
# actually add repo to the system db
sudo dpkg --add-architecture arm64 && \
sudo apt-get update && \

# arm64 dependencies to build amd64-to-arm64 cross-compiling llvm
# minimal set of NES dependencies in arm64 as well
sudo apt-get install -qq --no-install-recommends \
  libpython3-dev:arm64 \
  libncurses5-dev:arm64 \
  libxml2-dev:arm64 \
  libedit-dev:arm64 \
  libdwarf-dev:arm64 \
  libdw-dev:arm64 \
  liblog4cxx-dev:arm64 \
  libcpprest-dev:arm64 \
  libssl-dev:arm64 \
  libeigen3-dev:arm64 \
  libzmqpp-dev:arm64 \
  z3:arm64 \
  libz3-dev:arm64 && \
  sudo apt-get clean -qq && \

# git checkout later produces non-empty output
git config --global advice.detachedHead false && \

# clone Ubuntu LTS version, build for arm64
# libboost-all-dev cannot be installed due to python3.8-minimal:arm64
git clone --quiet --single-branch --recursive https://github.com/boostorg/boost && cd boost \
rm -rf more && git checkout boost-1.71.0 && ./bootstrap.sh && \
sed -i -- 's|using gcc ;|using gcc : arm64 : aarch64-linux-gnu-g++ ;|g' project-config.jam && \
./b2 install -j2 toolset=gcc-arm64 --prefix=/usr/local/boost && cd && \

# needed for copying libs to sysroot
# https://github.com/plotfi/llvm-pi/blob/master/ubuntu-docker-presetup.sh
GCC_VERSION=$(aarch64-linux-gnu-gcc -dumpversion) && \

# create sysroot and toolchain
sudo mkdir -p /opt/sysroots/aarch64-linux-gnu/usr && \
sudo mkdir -p /opt/toolchain && \

# copy std libs from Ubuntu's multi-arch gcc (for clang)
cd /opt/sysroots/aarch64-linux-gnu/usr && \
sudo cp -r -v -L /usr/aarch64-linux-gnu/include /usr/aarch64-linux-gnu/lib . && cd lib && \
sudo cp -r -v -L /usr/lib/gcc-cross/aarch64-linux-gnu/"$GCC_VERSION"/*gcc* . && \
sudo cp -r -v -L /usr/lib/gcc-cross/aarch64-linux-gnu/"$GCC_VERSION"/*crt* . && \
sudo cp -r -v -L /usr/lib/gcc-cross/aarch64-linux-gnu/"$GCC_VERSION"/libsupc++.a . && \
sudo cp -r -v -L /usr/lib/gcc-cross/aarch64-linux-gnu/"$GCC_VERSION"/libstdc++*  . && cd && \

# faster clone from github mirror instead of official site (v13 is default in ubuntu LTS)
git clone --branch llvmorg-10.0.0 --single-branch https://github.com/llvm/llvm-project && \
mkdir -p llvm-project/build && cd llvm-project/build && \
cmake -G Ninja \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=/opt/toolchain \
-DCMAKE_CROSSCOMPILING=ON \
-DLLVM_BUILD_DOCS=OFF \
-DLLVM_DEFAULT_TARGET_TRIPLE=aarch64-linux-gnu \
-DLLVM_TARGET_ARCH=AArch64 \
-DLLVM_TARGETS_TO_BUILD=AArch64 \
-DLLVM_ENABLE_PROJECTS="clang;compiler-rt;lld;polly;libcxx;libcxxabi;openmp" \
../llvm && \
ninja -j 2 clang && ninja -j 2 cxx && sudo ninja install && \
cd && \
sudo rm -rf llvm-project && \

# LOCAL build needs a toolchain file
cat > toolchain-aarch64-llvm.cmake <<'EOT' &&
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_TARGET_ABI linux-gnu)
SET(CROSS_TARGET ${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_TARGET_ABI})

# directory of custom-compiled llvm with cross-comp support
set(CROSS_LLVM_PREFIX /opt/toolchain)

# specify the cross compiler
set(CMAKE_C_COMPILER ${CROSS_LLVM_PREFIX}/bin/clang)
set(CMAKE_CXX_COMPILER ${CROSS_LLVM_PREFIX}/bin/clang++)

# specify sysroot (our default: ubuntu-arm)
SET(CMAKE_SYSROOT /opt/sysroots/aarch64-linux-gnu)

# base version of gcc to include from in the system
SET(CROSS_GCC_BASEVER "9")

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

sudo cp toolchain-aarch64-llvm.cmake /opt/toolchain/toolchain-aarch64-llvm.cmake && \

# build grpc for host architecture, necessary for crosscompiling later
# more here: https://chromium.googlesource.com/external/github.com/grpc/grpc/+/HEAD/test/distrib/cpp/run_distrib_test_raspberry_pi.sh
# and here: https://chromium.googlesource.com/external/github.com/grpc/grpc/+/HEAD/BUILDING.md
cd && git clone --branch v1.28.1 https://github.com/grpc/grpc.git && \
  cd grpc && git submodule update --init --recursive --jobs 1 && mkdir -p build && cd build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_SSL_PROVIDER=package \
  && make -j2 install && cd .. && rm -rf build && mkdir -p build && cd build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=/opt/toolchain/toolchain-aarch64-llvm.cmake \
  -DCMAKE_INSTALL_PREFIX=/opt/sysroot/aarch64-linux-gnu/grpc_install \
  && sudo make -j2 install && cd ../.. && sudo rm -rf grpc && \

cd ${HOME} && git clone https://github.com/eclipse/paho.mqtt.c.git && \
  cd paho.mqtt.c && git checkout v1.3.8 && \
  cmake -Bbuild -H. -DPAHO_ENABLE_TESTING=OFF \
  -DPAHO_BUILD_STATIC=ON \
  -DPAHO_WITH_SSL=ON \
  -DPAHO_HIGH_PERFORMANCE=ON && \
  sudo cmake --build build/ --target install && \
  sudo ldconfig && cd ${HOME} && rm -rf paho.mqtt.c && \
  git clone https://github.com/eclipse/paho.mqtt.cpp && cd paho.mqtt.cpp && \
  cmake -Bbuild -H. -DPAHO_BUILD_STATIC=ON -DPAHO_BUILD_DOCUMENTATION=TRUE -DPAHO_BUILD_SAMPLES=TRUE \
  -DCMAKE_TOOLCHAIN_FILE=/opt/toolchain/toolchain-aarch64-gcc.cmake && \
  sudo cmake --build build/ --target install && sudo ldconfig && cd ${HOME} && rm -rf paho.mqtt.cpp

sudo cd /opt/sysroots/aarch64-linux-gnu/usr && \
sudo cp -r -v -L /usr/aarch64-linux-gnu/include /usr/aarch64-linux-gnu/lib . && cd lib && \
sudo cp -r -v -L /usr/include/z3* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/zmq* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/log4* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/libdwarf* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/dwarf* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/elfutils* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/cpprest* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/pplx* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/aarch64-linux-gnu/openssl* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/openssl* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/libpaho-mqttpp3* /opt/sysroots/aarch64-linux-gnu/include/ && \
sudo cp -r -v -L /usr/include/libpaho-mqtt3as* /opt/sysroots/aarch64-linux-gnu/include/ && \

sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libz3* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libzmq* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/liblog4* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libdwarf* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libdw* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libebl* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libcpprest* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/cmake/cpprestsdk* /opt/sysroots/aarch64-linux-gnu/lib/cmake/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libcrypto* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libssl* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libcpprest* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libpaho-mqttpp3* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo cp -r -v -L /usr/lib/aarch64-linux-gnu/libpaho-mqtt3as* /opt/sysroots/aarch64-linux-gnu/lib/ && \
sudo sed -i -- 's|/usr/lib/aarch64-linux-gnu/cmake/cpprestsdk|/opt/sysroots/aarch64-linux-gnu/lib/cmake/cpprestsdk|g' /opt/sysroots/aarch64-linux-gnu/lib/cmake/cpprestsdk/cpprestsdk-targets.cmake && \
sudo sed -i -- 's|/lib/aarch64-linux-gnu/|/aarch64-linux-gnu/lib/|g' /opt/sysroots/aarch64-linux-gnu/lib/cmake/cpprestsdk/cpprestsdk-targets-none.cmake && \
sudo sed -i -- 's|/include/|/aarch64-linux-gnu/include/|g' /opt/sysroots/aarch64-linux-gnu/lib/cmake/cpprestsdk/cpprestsdk-targets.cmake && \
cd

RUN update-alternatives --install /usr/bin/clang clang /opt/toolchain/bin/clang 10 && \
    update-alternatives --install /usr/bin/clang++ clang++ /opt/toolchain/bin/clang++ 10 && \
    update-alternatives --install /usr/bin/cc cc /usr/bin/clang 20 && \
    update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++ 20 && \
    update-alternatives --set cc /usr/bin/clang && \
    update-alternatives --set c++ /usr/bin/clang++
