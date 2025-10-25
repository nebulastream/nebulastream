# Thin development image: includes the full toolchain and dev tools but does NOT
# build vcpkg dependencies during image creation.  Dependencies are built at
# container runtime via CMake manifest mode and cached in Docker volumes.
#
# Build:  docker build -f docker/dependency/Thin.dockerfile -t nes-dev-thin .
# Run:    docker run -v $(pwd):/nebulastream -w /nebulastream -it nes-dev-thin bash

# ---------------------------------------------------------------------------
# Stage 1: Download and extract prebuilt MLIR/Clang for all sanitizer variants
# ---------------------------------------------------------------------------
FROM alpine:latest AS llvm-download
ARG LLVM_VERSION=21-with-fix-173075
RUN apk update && apk add zstd curl
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then ARCH_NAME=arm64; \
    elif [ "$ARCH" = "x86_64" ]; then ARCH_NAME=x64; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    for SANITIZER in none address thread undefined; do \
        curl -fSL "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-${LLVM_VERSION}/nes-llvm-${LLVM_VERSION}-${ARCH_NAME}-${SANITIZER}-libcxx.tar.zstd" -o llvm.tar.zstd && \
        zstd --decompress llvm.tar.zstd --stdout | tar -x && \
        mv clang clang-${SANITIZER}-libcxx; \
        if [ "$ARCH_NAME" = "x64" ]; then \
            curl -fSL "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-${LLVM_VERSION}/nes-llvm-${LLVM_VERSION}-${ARCH_NAME}-${SANITIZER}-libstdcxx.tar.zstd" -o llvm.tar.zstd && \
            zstd --decompress llvm.tar.zstd --stdout | tar -x && \
            mv clang clang-${SANITIZER}-local; \
        else \
            mkdir -p clang-${SANITIZER}-local; \
        fi; \
    done

# ---------------------------------------------------------------------------
# Stage 2: Final thin image
# ---------------------------------------------------------------------------
FROM nebulastream/nes-development-base:latest
ARG ANTLR4_VERSION=4.13.2
ARG TARGETARCH
ENV NES_ARCH=${TARGETARCH}

# -- Install AWS CLI (for R2 binary cache) ----------------------------------
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        URL="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl "$URL" -o "awscliv2.zip" && \
    unzip -q awscliv2.zip && \
    ./aws/install -i /usr/local/aws-cli -b /usr/local/bin && \
    rm -rf awscliv2.zip aws

# -- Install dev tools (merged from Development.dockerfile) -----------------
RUN apt-get update -y && apt-get install -y \
        clang-format-${LLVM_TOOLCHAIN_VERSION} \
        clang-tidy-${LLVM_TOOLCHAIN_VERSION} \
        lldb-${LLVM_TOOLCHAIN_VERSION} \
        gdb \
        python3-venv \
        python3-bs4 \
        openjdk-21-jre-headless \
        bats \
        expect \
        jq \
        yq \
    && rm -rf /var/lib/apt/lists/*

# -- Install the stable and nightly Rust toolchain ----------------------------
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.90.0

RUN set -eux; \
    \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
        'amd64') \
            rustArch='x86_64-unknown-linux-gnu'; \
            rustupSha256='20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c'; \
            ;; \
        'arm64') \
            rustArch='aarch64-unknown-linux-gnu'; \
            rustupSha256='e3853c5a252fca15252d07cb23a1bdd9377a8c6f3efa01531109281ae47f841c'; \
            ;; \
        *) \
            echo >&2 "unsupported architecture: $arch"; \
            exit 1; \
            ;; \
    esac; \
    \
    url="https://static.rust-lang.org/rustup/archive/1.28.2/${rustArch}/rustup-init"; \
    wget --progress=dot:giga "$url"; \
    echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
    \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION} --default-host ${rustArch}; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    \
    rustup install nightly; \
    rustup component add rustfmt; \
    rustup component add rust-src --toolchain nightly; \
    rustup --version; \
    cargo --version; \
    rustc --version;

# Antlr JAR (required by the antlr4 vcpkg port)
ADD --checksum=sha256:eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76 --chmod=744 \
  https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar /opt/antlr-${ANTLR4_VERSION}-complete.jar

# ClangBuildAnalyzer
RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j \
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version

# GDB Libc++ Pretty Printer
RUN wget -P /usr/share/libcxx/ https://raw.githubusercontent.com/llvm/llvm-project/refs/tags/llvmorg-19.1.7/libcxx/utils/gdb/libcxx/printers.py && \
    cat <<EOF > /etc/gdb/gdbinit
    python
    import sys
    sys.path.insert(0, '/usr/share/libcxx')
    import printers
    printers.register_libcxx_printer_loader()
    end
EOF

# -- Copy prebuilt LLVM variants from download stage ------------------------
COPY --from=llvm-download /clang-none-libcxx      /clang-none-libcxx
COPY --from=llvm-download /clang-address-libcxx   /clang-address-libcxx
COPY --from=llvm-download /clang-thread-libcxx    /clang-thread-libcxx
COPY --from=llvm-download /clang-undefined-libcxx /clang-undefined-libcxx
COPY --from=llvm-download /clang-none-local       /clang-none-local
COPY --from=llvm-download /clang-address-local    /clang-address-local
COPY --from=llvm-download /clang-thread-local     /clang-thread-local
COPY --from=llvm-download /clang-undefined-local  /clang-undefined-local

# -- Copy vcpkg manifest and ports (NOT built) ------------------------------
ADD vcpkg /vcpkg_input
ENV VCPKG_FORCE_SYSTEM_BINARIES=1

# -- Bootstrap vcpkg (just the binary, not dependencies) --------------------
RUN cd /vcpkg_input \
    && git clone https://github.com/microsoft/vcpkg.git vcpkg_repository \
    && vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics \
    && chmod -R a+rwX /vcpkg_input

# -- Create volume mount points with correct permissions --------------------
RUN mkdir -p /vcpkg-binary-cache /ccache /test-file-cache \
    && chmod 777 /vcpkg-binary-cache /ccache /test-file-cache

# -- Environment: CMake will use manifest mode via VCPKG_ROOT ---------------
ENV VCPKG_ROOT=/vcpkg_input/vcpkg_repository

# -- Entrypoint: configures R2 binary cache at runtime ----------------------
COPY docker/dependency/nes-thin-entrypoint.sh /usr/local/bin/nes-thin-entrypoint.sh
RUN chmod +x /usr/local/bin/nes-thin-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/nes-thin-entrypoint.sh"]
CMD ["bash"]
