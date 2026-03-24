# syntax=docker/dockerfile:1
# The Development CI image installs a github runner specific user
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

# The UID env var should be used in child Containerfile.
ENV UID=1001
ENV GID=1001
ENV USERNAME="runner"
USER root
# Install sccache from GitHub releases (the Ubuntu apt package has S3 backend disabled)
ARG SCCACHE_VERSION=0.14.0
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then TARGET="x86_64-unknown-linux-musl"; \
    elif [ "$ARCH" = "aarch64" ]; then TARGET="aarch64-unknown-linux-musl"; fi && \
    curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${TARGET}.tar.gz" | tar xz -C /tmp && \
    cp "/tmp/sccache-v${SCCACHE_VERSION}-${TARGET}/sccache" /usr/local/bin/sccache && \
    chmod +x /usr/local/bin/sccache && \
    rm -rf /tmp/sccache-* && \
    sccache --version
RUN groupadd --gid ${GID} runner && useradd $USERNAME --gid ${GID} --uid ${UID}
RUN mkdir -p /home/${USERNAME} \
    && chown -R $USERNAME:$GID /home/runner

WORKDIR /home/runner
ENV HOME=/home/${USERNAME}
ENV MOLD_JOBS=1
USER 1001:1001
