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

# ODBC driver manager plus the Microsoft SQL Server driver, so the ODBC source can
# reach the study's MSSQL instance. msodbcsql18 registers itself in /etc/odbcinst.ini,
# which is why ODBCSource points ODBCSYSINI at /etc (vcpkg's static unixODBC does not
# default there). mssql-tools18 provides sqlcmd for poking at the DB by hand.
RUN apt update -y && apt install unixodbc -y
RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools18

RUN (${ROOTLESS} || (echo "uid: ${UID} gid ${GID} username ${USERNAME}" && \
    (delgroup ubuntu || true) && \
    (deluser ubuntu || true) && \
    addgroup --gid ${GID} ${USERNAME} && \
    adduser --uid ${UID} --gid ${GID} ${USERNAME} && \
    chown -R ${UID}:${GID} ${NES_PREBUILT_VCPKG_ROOT}))

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
