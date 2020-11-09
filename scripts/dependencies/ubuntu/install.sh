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
  libssl-dev \
  clang-format \
  librdkafka1 \
  librdkafka++1 \
  librdkafka-dev \
  libeigen3-dev \
  libzmqpp-dev \
  git \
  wget \
  z3 \
  tar

sudo add-apt-repository ppa:open62541-team/ppa -qq && \
  sudo apt-get update && \
  sudo apt-get install libopen62541-1-dev -qq

git clone --branch v1.28.1 https://github.com/grpc/grpc.git && \
  cd grpc && git submodule update --init --jobs 1 && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && \
  make -j && sudo make install && cd .. && cd .. && rm -rf grpc



