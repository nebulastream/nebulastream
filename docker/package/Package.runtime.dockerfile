# syntax=docker/dockerfile:1
#
# Generic "package" runtime image for a single, already-built NebulaStream binary.
#
# Unlike the dockerfiles in docker/frontend and docker/single-node-worker, this
# dockerfile does NOT rebuild anything inside Docker. It simply COPIES a binary
# that was produced by the current build tree (see the `package-docker-images`
# CMake target) into the slim runtime base image.
#
# The build context handed to `docker build` is a small staging directory that
# contains exactly one file: the prebuilt binary, named ${BIN_NAME}. The CMake
# target populates that staging directory via `$<TARGET_FILE:...>`.
#
# Caveat: because the binary is copied (not rebuilt), the binary must be ABI
# compatible with the runtime base image (libc / libc++ versions). This is
# guaranteed when the binary is produced with the Docker-based toolchain
# (nebulastream/nes-development), but local host builds may hit version
# mismatches against nebulastream/nes-runtime-base.
ARG RUNTIME_TAG=latest
FROM nebulastream/nes-runtime-base:${RUNTIME_TAG} AS app

# Name of the binary to package. Provided by the CMake target; it is both the
# file expected in the build context and the resulting entrypoint.
ARG BIN_NAME

# Mirror the worker/cli runtime layout: binaries live in /usr/bin.
COPY ${BIN_NAME} /usr/bin/${BIN_NAME}

# State is kept on a volume so that stateful binaries (e.g. nes-cli) persist
# across container restarts, matching docker/frontend/cli.dockerfile.
VOLUME /state
ENV XDG_STATE_HOME=/state

# ENTRYPOINT cannot expand a build ARG directly, so the CMake target bakes the
# concrete binary name in via the BIN_NAME env var consumed by this wrapper.
ENV NES_BIN_NAME=${BIN_NAME}
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/bin/${NES_BIN_NAME} \"$@\"", "--"]
