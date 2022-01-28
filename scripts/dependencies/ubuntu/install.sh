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

#! /bin/bash
sudo apt-get update -qq && sudo apt-get install -qq \
  libdwarf-dev \
  libdwarf1 \
  binutils-dev \
  libdw-dev \
  libssl-dev \
  build-essential \
  clang-format \
  libmbedtls-dev \
  libjemalloc-dev \
  git \
  wget \
  python3.8 \
  libsodium-dev \
  tar \
  libbsd-dev \
  p7zip \
  doxygen \
  graphviz \
  software-properties-common

cd ${HOME} && wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null \
    && sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ focal main' \
    && sudo apt update && sudo apt install -y kitware-archive-keyring && rm /etc/apt/trusted.gpg.d/kitware.gpg && sudo apt update && sudo apt install -y cmake

sudo add-apt-repository ppa:open62541-team/ppa && \
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

## folly
sudo apt-get install -qq \
    g++ \
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
