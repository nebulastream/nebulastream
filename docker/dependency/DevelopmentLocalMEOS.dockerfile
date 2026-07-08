# syntax=docker/dockerfile:1
# Adds the MEOS library (and its geospatial dependencies) on top of the standard
# nes-development image, so the MEOS plugin (nes-plugins/MEOS) can be built.
#
# This is an OPTIONAL layer inserted between Development.dockerfile (:default) and
# DevelopmentLocal.dockerfile by `install-local-docker-environment.sh --meos`.
# It intentionally builds FROM the full development image so it inherits the whole
# toolchain (clang, rust, docker CLI, iree, ...) and only adds what MEOS needs.
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

USER root

# MEOS build/runtime dependencies (matches nes-plugins/MEOS/CMakeLists.txt: GEOS, PROJ, GSL, json-c)
RUN apt-get update -y && apt-get install -y \
        libgeos-dev \
        libproj-dev \
        libgsl-dev \
        libjson-c-dev \
        autoconf \
        automake \
        libtool \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Build and install MEOS from source into /usr/local, where cmake/Findmeos.cmake looks
# (meos.h + libmeos.so). Pinned to the commit the plugin is developed against; override
# MEOS_COMMIT to bump. Build flags mirror a local `cmake -B build -S .` Release install.
ARG MEOS_REPO=https://github.com/MobilityDB/MEOS
ARG MEOS_COMMIT=43ca4c6
RUN set -eux; \
    git clone "${MEOS_REPO}" /tmp/meos-src; \
    cd /tmp/meos-src; \
    git checkout "${MEOS_COMMIT}"; \
    cmake -B build -S . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local; \
    cmake --build build -j"$(nproc)"; \
    cmake --install build; \
    ldconfig; \
    rm -rf /tmp/meos-src
