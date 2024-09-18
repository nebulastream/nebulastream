# This image is build locally on a developers machine. It installs the current user into the container instead of
# relying on the root user. Ubuntu 24 by default installs the ubuntu user which is replaced.
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

USER root
ARG UID=1000
ARG GID=1000
ARG USERNAME=ubuntu
ARG ROOTLESS=false
RUN (${ROOTLESS} || (echo "uid: ${UID} gid ${GID} username ${USERNAME}" && groupdel ubuntu && deluser --remove-home ubuntu && \
    addgroup --gid ${GID} ${USERNAME} && \
    adduser --uid ${UID} --gid ${GID} ${USERNAME})) && \
    chown -R ${UID}:${GID} ${NES_PREBUILT_VCPKG_ROOT}

USER ${USERNAME}
