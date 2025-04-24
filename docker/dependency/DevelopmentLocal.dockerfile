# This image is build locally on a developers machine. It installs the current user into the container instead of
# relying on the root user. Ubuntu 24 by default installs the ubuntu user which is replaced.
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

USER root
ARG UID=1000
ARG GID=1000
ARG USERNAME=ubuntu
ARG ROOTLESS=false

# Installing perf from source as the version in the ubuntu repository might not use the same kernel version as the host
# Also the perf version in the ubuntu repository might not be compatible with the host kernel version
RUN apt install -y \
    linux-tools-common \
    python3\
    python3-dev\
    libdw-dev\
    libunwind-dev\
    flex\
    bison\
    git\
    pkg-config\
    libelf-dev\
    libtraceevent-dev

RUN git clone --depth 1 https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git \
    && old_path=`pwd` \
    && cd linux \
    && git fetch --all --tags \
    && git checkout v`uname -r | awk -F'[.-]' '{print $1"."$2}'` \
    && cd tools/perf \
    && NO_JEVENTS=1 make -j$(nproc) perf \
    && cp perf /usr/bin/ \
    && chmod +x /usr/bin/perf \
    && perf --version \
    && cd $old_path \
    && rm -rf linux

RUN (${ROOTLESS} || (echo "uid: ${UID} gid ${GID} username ${USERNAME}" && \
    (delgroup ubuntu || true) && \
    (deluser ubuntu || true) && \
    addgroup --gid ${GID} ${USERNAME} && \
    adduser --uid ${UID} --gid ${GID} ${USERNAME})) && \
    chown -R ${UID}:${GID} ${NES_PREBUILT_VCPKG_ROOT}

USER ${USERNAME}
