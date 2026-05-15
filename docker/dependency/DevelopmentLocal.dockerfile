# syntax=docker/dockerfile:1
# This image is build locally on a developers machine. It installs the current user into the container instead of
# relying on the root user. Ubuntu 24 by default installs the ubuntu user which is replaced.
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

USER root
ARG UID=1000
ARG GID=1000
ARG USERNAME=ubuntu
ARG ROOTLESS=false
# GID of the host's docker group. When set (and not in rootless mode), a
# `docker` group with this GID is created inside the image and the container
# user is added to it. Combined with a bind-mounted /var/run/docker.sock and
# `--group-add docker` at run time, that gives the user docker-daemon access
# without per-developer numeric --group-add hacks.
#
# install-local-docker-environment.sh detects the host's docker GID and
# passes it here. If detection fails (no host docker group, or unsupported
# platform like macOS where this whole path is skipped via ROOTLESS=true),
# DOCKER_GID stays empty and the group is not created — `docker` commands
# from inside the container simply won't work, which is the honest result.
ARG DOCKER_GID=

RUN (${ROOTLESS} || (echo "uid: ${UID} gid ${GID} username ${USERNAME}" && \
    (delgroup ubuntu || true) && \
    (deluser ubuntu || true) && \
    addgroup --gid ${GID} ${USERNAME} && \
    adduser --uid ${UID} --gid ${GID} ${USERNAME} && \
    chown -R ${UID}:${GID} ${NES_PREBUILT_VCPKG_ROOT}))

# Permission checks on the bind-mounted /var/run/docker.sock are by GID, so
# the in-image group only has to match numerically; the name `docker` then
# works with `--group-add docker` portably. If a pre-existing group already
# holds the same GID inside the base image, rename it to `docker` rather
# than allocating a second group with the same GID (which addgroup refuses).
RUN (${ROOTLESS} || [ -z "${DOCKER_GID}" ] || ( \
    (getent group docker >/dev/null && groupdel docker) || true; \
    existing_name=$(getent group "${DOCKER_GID}" | cut -d: -f1); \
    if [ -n "$existing_name" ]; then \
        groupmod -n docker "$existing_name"; \
    else \
        addgroup --gid ${DOCKER_GID} docker; \
    fi && \
    usermod -aG docker ${USERNAME}))

# Create containerd socket directory with appropriate permissions
RUN mkdir -p /run/containerd && \
    chown -R ${UID}:${GID} /run/containerd

USER ${USERNAME}
