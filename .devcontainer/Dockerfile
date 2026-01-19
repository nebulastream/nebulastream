# Development container for NebulaStream
# This Dockerfile creates a non-root user for VS Code Dev Containers compatibility

ARG BASE_IMAGE=nebulastream/nes-development:main-libcxx-none
FROM ${BASE_IMAGE}

# Create non-root user for development (VS Code Dev Containers expects this)
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

USER root

# Create user and group, handling the case where they might already exist
RUN if getent group ${USER_GID} > /dev/null 2>&1; then \
        groupmod -n ${USERNAME} $(getent group ${USER_GID} | cut -d: -f1) 2>/dev/null || true; \
    else \
        groupadd --gid ${USER_GID} ${USERNAME}; \
    fi \
    && if getent passwd ${USER_UID} > /dev/null 2>&1; then \
        usermod -l ${USERNAME} -d /home/${USERNAME} -m $(getent passwd ${USER_UID} | cut -d: -f1) 2>/dev/null || true; \
    else \
        useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME}; \
    fi \
    && mkdir -p /home/${USERNAME} \
    && chown -R ${USER_UID}:${USER_GID} /home/${USERNAME} \
    && chown -R ${USER_UID}:${USER_GID} ${NES_PREBUILT_VCPKG_ROOT:-/vcpkg}

USER ${USERNAME}
WORKDIR /workspaces/nebulastream
