FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        libmosquitto-dev \
        libyaml-cpp-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY CMakeLists.txt ./
COPY datagen_batch_new.cpp ./
COPY configLoader.cpp ./
COPY configLoader.hpp ./
COPY Util ./Util
COPY include ./include
COPY asio-1.30.2 ./asio-1.30.2

RUN cmake -S . -B /build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /build --config Release -j"$(nproc)"

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libmosquitto1 \
        libyaml-cpp0.8 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/datagen /usr/local/bin/datagen
COPY docker/entrypoint.sh /usr/local/bin/datagen-entrypoint

RUN chmod +x /usr/local/bin/datagen-entrypoint \
    && mkdir -p /app/config /app/data

ENV DATAGEN_CONFIG=/app/config/config.yaml

ENTRYPOINT ["/usr/local/bin/datagen-entrypoint"]
