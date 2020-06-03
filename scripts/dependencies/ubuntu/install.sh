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
  git \
  wget \
  tar

git clone --branch v1.28.1 https://github.com/grpc/grpc.git && \
  cd grpc && git submodule update --init --jobs 1 && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && \
  make -j && sudo make install && cd .. && cd .. && rm -rf grpc && \
  git clone --single-branch --branch v4.3.2 https://github.com/zeromq/libzmq.git && cd libzmq && \
  mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j && sudo make install && \
  cd .. && cd .. && rm -rf libzmq && \
  git clone --single-branch --branch v4.6.0 https://github.com/zeromq/cppzmq.git && cd cppzmq && \
  mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j && sudo make install && \
  cd .. && cd .. && rm -rf cppzmq



