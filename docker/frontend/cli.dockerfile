# syntax=docker/dockerfile:1
ARG TAG=latest
ARG RUNTIME_TAG=${TAG}
FROM nebulastream/nes-development:${TAG} AS build
ARG BUILD_TYPE=RelWithDebInfo
ARG NES_LOG_LEVEL=WARN
ARG USE_SANITIZER=none
ARG NES_LIBCXX_HARDENING_MODE=AUTO

USER root
ADD . /home/ubuntu/src
RUN --mount=type=cache,id=ccache,target=/ccache \
    export CCACHE_DIR=/ccache && \
    cd /home/ubuntu/src \
    && cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 \
        -DNES_SPLIT_TEST_BINARIES=OFF -DNES_LOG_LEVEL=${NES_LOG_LEVEL} -DUSE_SANITIZER=${USE_SANITIZER} \
        -DNES_LIBCXX_HARDENING_MODE=${NES_LIBCXX_HARDENING_MODE} \
    && MOLD_JOBS=1 cmake --build build --target nes-cli -j \
    && mkdir /tmp/bin \
    && find build -name 'nes-cli' -type f -exec mv --target-directory=/tmp/bin {} +

FROM nebulastream/nes-runtime-base:${RUNTIME_TAG} AS app
VOLUME /state
ENV XDG_STATE_HOME=/state
COPY --from=build /tmp/bin /usr/bin
ENTRYPOINT ["nes-cli"]
