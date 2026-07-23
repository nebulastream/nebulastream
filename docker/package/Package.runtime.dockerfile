# syntax=docker/dockerfile:1
#
# Single parametrized dockerfile that packages one NebulaStream binary into the
# slim runtime base image (nebulastream/nes-runtime-base). It replaces the four
# near-identical per-binary dockerfiles that used to live in docker/frontend and
# docker/single-node-worker.
#
# Two modes, selected via BuildKit stage selection (`docker build --target ...`):
#
#   --target build-in-docker
#       Compiles ${BINARY_TARGET} inside nebulastream/nes-development:${TAG}
#       (build context = repository root) and copies the resulting binary into
#       the runtime image. This is what CI and `NES_DOCKER_BUILD_IN_DOCKER=1`
#       use; the toolchain always matches the runtime base image.
#
#   --target prebuilt
#       COPYs an already-built binary from the build context. The context is a
#       small staging directory containing exactly one file named ${BIN_NAME},
#       populated by the `package-docker-*` CMake targets via `$<TARGET_FILE:...>`.
#       Caveat: the binary must be ABI compatible with the runtime base image
#       (libc / libc++ versions). This is guaranteed when it was produced with
#       the Docker-based toolchain (nebulastream/nes-development); local host
#       builds may hit version mismatches.
ARG TAG=latest
ARG RUNTIME_TAG=latest

# Build stage: only materialized for --target build-in-docker (BuildKit skips it
# for --target prebuilt).
FROM nebulastream/nes-development:${TAG} AS build
# CMake target to build; its output binary is named ${BIN_NAME}.
ARG BINARY_TARGET
ARG BIN_NAME=${BINARY_TARGET}
ARG BUILD_TYPE=RelWithDebInfo

USER root
ADD . /home/ubuntu/src
RUN --mount=type=cache,id=ccache,target=/ccache \
    export CCACHE_DIR=/ccache && \
    cd /home/ubuntu/src && \
    cmake -B build -S . -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DNES_ENABLES_TESTS=0 && \
    cmake --build build --target ${BINARY_TARGET} -j && \
    mkdir /tmp/bin && \
    find build -name "${BIN_NAME}" -type f -exec mv --target-directory=/tmp/bin {} +

# Shared runtime layer: binaries live in /usr/bin, state is kept on a volume so
# that stateful binaries (e.g. nes-cli) persist across container restarts.
FROM nebulastream/nes-runtime-base:${RUNTIME_TAG} AS runtime
ARG BIN_NAME
VOLUME /state
ENV XDG_STATE_HOME=/state
# ENTRYPOINT cannot expand a build ARG directly, so the concrete binary name is
# baked in via the BIN_NAME env var consumed by this wrapper.
ENV NES_BIN_NAME=${BIN_NAME}
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/bin/${NES_BIN_NAME} \"$@\"", "--"]

# Mode 1: copy the binary compiled in the build stage above.
FROM runtime AS build-in-docker
ARG BIN_NAME
COPY --from=build /tmp/bin/${BIN_NAME} /usr/bin/${BIN_NAME}

# Mode 2 (default): copy a prebuilt binary from the staging-directory context.
FROM runtime AS prebuilt
ARG BIN_NAME
COPY ${BIN_NAME} /usr/bin/${BIN_NAME}
