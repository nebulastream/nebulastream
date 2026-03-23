ARG TAG=latest
ARG BUILD_TYPE=RelWithDebInfo
ARG RUNTIME_TAG=${TAG}
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN --mount=type=cache,id=ccache,target=/ccache \
    export CCACHE_DIR=/ccache && \
    cd /home/ubuntu/src \
    && cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 \
    && cmake --build build --target nes-cli -j \
    && mkdir /tmp/bin \
    && find build -name 'nes-cli' -type f -exec mv --target-directory=/tmp/bin {} +

FROM nebulastream/nes-runtime-base:${RUNTIME_TAG} AS app
VOLUME /state
ENV XDG_STATE_HOME=/state
COPY --from=build /tmp/bin /usr/bin
ENTRYPOINT ["nes-cli"]
