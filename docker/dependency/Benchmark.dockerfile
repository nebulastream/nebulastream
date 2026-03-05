# The benchmarking image adds common benchmarking tools and python modules need to run the benchmarking scripts and
# upload the results
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

RUN apt-get update && apt-get install python3-pip -y

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

RUN python3 -m pip install benchadapt benchrun --break-system-packages
