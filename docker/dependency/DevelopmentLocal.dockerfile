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
# the in-image group only has to match numerically. Two paths:
#
#   1. No group in the base image owns DOCKER_GID yet → create a `docker`
#      group at that GID and add the user. `--group-add docker` works
#      portably at run time.
#
#   2. Another group already owns DOCKER_GID (e.g. on Ubuntu hosts where
#      docker installs at GID 998, the base image's `systemd-network`
#      already squats 998) → add the user to that existing group instead
#      of renaming the squatter. The user picks up the numeric GID as a
#      supplementary group, so socket access works at run time without any
#      `--group-add` flag. The squatter's name and privileges are
#      untouched, which is why we don't `groupmod -n` here — silently
#      renaming would break anything in the image that resolves the group
#      by name (the user's primary group, package post-install hooks,
#      build scripts that grep group names, etc.).
RUN (${ROOTLESS} || [ -z "${DOCKER_GID}" ] || ( \
    (getent group docker >/dev/null && groupdel docker) || true; \
    existing_name=$(getent group "${DOCKER_GID}" | cut -d: -f1); \
    if [ -n "$existing_name" ]; then \
        echo "Note: DOCKER_GID=${DOCKER_GID} is already used by group '${existing_name}' in the base image." >&2; \
        echo "      Adding ${USERNAME} to '${existing_name}' for socket access — no `docker` group will exist in the image." >&2; \
        usermod -aG "${existing_name}" ${USERNAME}; \
    else \
        addgroup --gid ${DOCKER_GID} docker && \
        usermod -aG docker ${USERNAME}; \
    fi))

# Create containerd socket directory with appropriate permissions
RUN mkdir -p /run/containerd && \
    chown -R ${UID}:${GID} /run/containerd

USER ${USERNAME}
