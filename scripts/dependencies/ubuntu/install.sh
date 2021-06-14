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

#! /bin/bash

sudo apt-get update -qq && sudo apt-get install -qq \
  clang \
  libdwarf-dev \
  libdwarf1 \
  llvm-dev \
  binutils-dev \
  libdw-dev \
  libboost-all-dev \
  liblog4cxx-dev \
  libcpprest-dev \
  libmbedtls-dev \
  libssl-dev \
  libjemalloc-dev \
  clang-format \
  librdkafka1 \
  librdkafka++1 \
  librdkafka-dev \
  libeigen3-dev \
  libzmqpp-dev \
  doxygen \
  graphviz \
  git \
  wget \
  z3 \
  tar

wget https://github.com/Kitware/CMake/archive/refs/tags/v3.18.5.tar.gz
tar -zxvf v3.18.5.tar.gz
cd CMake-3.18.5 && ./bootstrap && make -j && sudo make -j install && cd .. && rm -rf CMake-3.18.5 && rm v3.18.5.tar.gz

sudo add-apt-repository ppa:open62541-team/ppa -qq && \
  sudo apt-get update && \
  sudo apt-get install libopen62541-1-dev -qq

cd ${HOME} && git clone https://github.com/eclipse/paho.mqtt.c.git && \
  cd paho.mqtt.c && git checkout v1.3.8 && \
  cmake -Bbuild -H. -DPAHO_ENABLE_TESTING=OFF \
  -DPAHO_BUILD_STATIC=ON \
  -DPAHO_WITH_SSL=ON \
  -DPAHO_HIGH_PERFORMANCE=ON && \
  sudo cmake --build build/ --target install && \
  sudo ldconfig && cd ${HOME} && rm -rf paho.mqtt.c && \
  git clone https://github.com/eclipse/paho.mqtt.cpp && cd paho.mqtt.cpp && \
  cmake -Bbuild -H. -DPAHO_BUILD_STATIC=ON -DPAHO_BUILD_DOCUMENTATION=TRUE -DPAHO_BUILD_SAMPLES=TRUE && \
  sudo cmake --build build/ --target install && sudo ldconfig && cd ${HOME} && sudo rm -rf paho.mqtt.cpp

git clone --branch v1.28.1 https://github.com/grpc/grpc.git && \
  cd grpc && git submodule update --init --jobs 1 && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && \
  make -j3 && sudo make install && cd .. && cd .. && rm -rf grpc


## folly
sudo apt-get install -qq \
    g++ \
    cmake \
    libboost-all-dev \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libiberty-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev \
    pkg-config \
    libunwind-dev

git clone https://github.com/fmtlib/fmt.git && cd fmt && \
    mkdir _build && cd _build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && \
    sudo make install && cd ../.. && git clone https://github.com/facebook/folly.git && \
    cd folly && mkdir _build && cd _build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && sudo make install && cd ../.. && rm -rf fmt folly
