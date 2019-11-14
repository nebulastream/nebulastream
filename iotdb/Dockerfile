FROM ubuntu:18.04

RUN apt-get update -y && apt-get remove -y llvm-6.0 && \
    apt-get install -y git cmake clang-3.9 llvm-3.9-dev libclang-3.9-dev
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-3.9 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-3.9 100
RUN apt-get install -y libzmq3-dev libboost1.65-all-dev liblog4cxx-dev libssl-dev libcpprest-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN cd ~ && \
    git clone https://github.com/actor-framework/actor-framework.git && \
    cd actor-framework && \
    ./configure && cd build && make && make install

ADD . /iotdb
WORKDIR /iotdb

EXPOSE 4711
EXPOSE 8081
EXPOSE 38938

CMD ["docker/scripts/entrypoint.sh"]