# No `# syntax=` directive on purpose: this file uses only pre-18.09 Dockerfile instructions, and
# pinning an external frontend would make every local rebuild pull docker/dockerfile:1 from Docker
# Hub -- which breaks switching between already-downloaded dependency images while offline.
# Re-add it if you ever need BuildKit-only syntax (--mount, --checksum, heredocs) in this file.
# This image is build locally on a developers machine. It installs the current user into the container instead of
# relying on the root user. Ubuntu 24 by default installs the ubuntu user which is replaced.
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

USER root
ARG UID=1000
ARG GID=1000
ARG USERNAME=ubuntu
ARG ROOTLESS=false

# The vcpkg root is made world-writable rather than chown -R'd: the export is ~2.8GB across ~16k
# files, and rewriting every inode would store the whole tree a second time (~2.9GB of image).
# Dependency.dockerfile already ran `chmod -R g=u,o=u` on it, so every file below is world-readable
# and world-writable; only the /vcpkg root itself was 0755, which this single-inode chmod fixes.
RUN (${ROOTLESS} || (echo "uid: ${UID} gid ${GID} username ${USERNAME}" && \
    (delgroup ubuntu || true) && \
    (deluser ubuntu || true) && \
    addgroup --gid ${GID} ${USERNAME} && \
    adduser --uid ${UID} --gid ${GID} ${USERNAME} && \
    chmod 0777 ${NES_PREBUILT_VCPKG_ROOT}))

# Create containerd socket directory with appropriate permissions
RUN mkdir -p /run/containerd && \
    chown -R ${UID}:${GID} /run/containerd

# Mirror the root-level gdbinit trust setting for the non-root dev user: GDB reads
# per-user config from ~/.config/gdb/gdbinit, and without this it refuses to
# auto-load the repo-root .gdbinit (which loads the pretty-printer) when running
# as ${USERNAME}.
RUN mkdir -p /home/${USERNAME}/.config/gdb && \
    echo "set auto-load safe-path /" > /home/${USERNAME}/.config/gdb/gdbinit
USER ${USERNAME}
