# syntax=docker/dockerfile:1
ARG TAG=latest
ARG BUILD_TYPE=RelWithDebInfo
ARG RUNTIME_TAG=${TAG}
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN --mount=type=cache,id=ccache,target=/ccache \
    export CCACHE_DIR=/ccache && \
    cd /home/ubuntu/src && \
    cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 && \
    cmake --build build --target nes-single-node-worker -j && \
    mkdir /tmp/bin && \
    find build -name 'nes-single-node-worker' -type f -exec mv --target-directory=/tmp/bin {} +

FROM nebulastream/nes-runtime-base:${RUNTIME_TAG} AS app

# Add ODBC runtime dependencies for the ODBCSource plugin (MSSQL via msodbcsql18 plus unixodbc).
# libc++/grpc_health_probe are already provided by nes-runtime-base.
RUN apt update -y && apt install -y curl gpg \
    && curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && apt update -y \
    && ACCEPT_EULA=Y apt install -y msodbcsql18 mssql-tools18 unixodbc \
    && apt clean && rm -rf /var/lib/apt/lists/*


COPY --from=build /tmp/bin /usr/bin
ENTRYPOINT ["nes-single-node-worker"]
