# syntax=docker/dockerfile:1
# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest

# `COPY --from` does not support variable expansion in the image reference, so the `uv` source
# image is pinned via a globally-scoped ARG and resolved through a named stage (see the COPY below).
ARG UV_VERSION=0.11.17
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM nebulastream/nes-development-dependency:${TAG}

ARG ANTLR4_VERSION=4.13.2

RUN apt-get update -y && apt-get install -y \
        clang-format-${LLVM_TOOLCHAIN_VERSION} \
        clang-tidy-${LLVM_TOOLCHAIN_VERSION} \
        lldb-${LLVM_TOOLCHAIN_VERSION} \
        gdb \
        python3-venv \
        python3-bs4 \
        jq \
        yq \
        bats \
        bats-support \
        bats-assert \
        bats-file \
        unixodbc-dev \
        odbc-postgresql \
        openjdk-21-jre-headless

# Additional testing libraries for bats are discovered via the BATS_LIB_PATH environemnt
ENV BATS_LIB_PATH=/usr/lib/bats

# The vcpkg port of antlr requires the jar to be available somewhere
ADD --checksum=sha256:eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76 --chmod=744 \
  https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar /opt/antlr-${ANTLR4_VERSION}-complete.jar

RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j\
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version

# Install GDB Libc++ Pretty Printer
RUN wget -P /usr/share/libcxx/  https://raw.githubusercontent.com/llvm/llvm-project/refs/tags/llvmorg-19.1.7/libcxx/utils/gdb/libcxx/printers.py && \
    cat <<EOF > /etc/gdb/gdbinit
    python
    import sys
    sys.path.insert(0, '/usr/share/libcxx')
    import printers
    printers.register_libcxx_printer_loader()
    end
EOF

# Installing the stable and nightly rust toolchain
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

# Installing the stable and nightly rust toolchain
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.90.0

# Install IREE compiler tools for ML inference (ONNX → IREE compilation)
ARG IREE_COMPILER_VERSION=3.11.0
RUN python3 -m venv /opt/iree && \
    /opt/iree/bin/pip install --no-cache-dir \
        iree-base-compiler==${IREE_COMPILER_VERSION} \
        iree-turbine \
        onnx && \
    ln -s /opt/iree/bin/iree-compile /usr/local/bin/iree-compile && \
    ln -s /opt/iree/bin/iree-import-onnx /usr/local/bin/iree-import-onnx && \
    iree-compile --version

# Pre-clone Corrosion at the exact ref CMake will request, so offline configures inside the
# container can fall back to it when GitHub is unreachable. EnableRust.cmake probes GitHub
# first and only uses CORROSION_SRC when the probe fails. Tracks the nebulastream fork that
# avoids always-dirty Rust target rebuilds.
ENV CORROSION_GIT_REPO=https://github.com/nebulastream/corrosion.git \
    CORROSION_VERSION=v0.6.1-always-dirty-fix \
    CORROSION_SRC=/opt/corrosion
RUN git clone --depth 1 --branch ${CORROSION_VERSION} \
        ${CORROSION_GIT_REPO} ${CORROSION_SRC} && \
    chmod -R a+rX ${CORROSION_SRC}

# Pre-fetch every crate pinned in nes-network/Cargo.lock into $CARGO_HOME so cargo can build
# offline. Only manifests + lockfile are copied (filtered by .dockerignore); stub lib sources
# let cargo resolve the workspace without the real source tree.
COPY nes-network /tmp/nes-network
RUN set -eux; \
    find /tmp/nes-network -name Cargo.toml | while read m; do \
        d=$(dirname "$m"); \
        mkdir -p "$d/src"; \
        echo 'fn _stub() {}' > "$d/src/lib.rs"; \
        echo 'fn _stub() {}' > "$d/lib.rs"; \
    done; \
    cd /tmp/nes-network && cargo fetch --locked; \
    # Corrosion runs `cargo install cxxbridge-cmd --version <cxx-version> --locked` at build \
    # time to generate the cxx C++/Rust bindings. Pre-install it here with the same `--locked` \
    # so its .crate sources and the transitive deps pinned by cxxbridge-cmd's bundled \
    # Cargo.lock end up in $CARGO_HOME for offline builds. Without `--locked` here, cargo \
    # resolves transitive deps freely and may cache different patch versions than the \
    # `--locked` install at runtime asks for, leading to offline download failures. \
    # The version is read from Cargo.lock so the cxxbridge-cmd version always matches the \
    # workspace-pinned cxx crate. \
    CXX_VERSION=$(awk '/^name = "cxx"$/{f=1; next} f && /^version = /{gsub(/"/,"",$3); print $3; exit}' Cargo.lock); \
    test -n "$CXX_VERSION"; \
    echo "Pre-installing cxxbridge-cmd ${CXX_VERSION}"; \
    cargo install cxxbridge-cmd --version "${CXX_VERSION}" --locked --root /tmp/cxxbridge-stage; \
    rm -rf /tmp/cxxbridge-stage; \
    cd / && rm -rf /tmp/nes-network; \
    chmod -R a+rwX ${CARGO_HOME}

# Install Docker CLI and Docker Compose for Docker-in-Docker testing
RUN apt-get update && \
    apt-get install -y \
        ca-certificates \
        curl \
        gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli docker-compose-plugin && \
    rm -rf /var/lib/apt/lists/* && \
    docker --version && \
    docker compose version

# Ship `uv` for the conn-test pytest runner (nes-conn-test/runner) so the
# battery no longer bootstraps it from astral.sh on every CI invocation —
# the `conntest` CLI keeps the curl bootstrap only as a fallback for older
# local images that predate this layer. Pinned, copied from the official
# distroless image (the recommended container install path). The version is
# pinned by the globally-scoped UV_VERSION ARG and the `uv` stage at the top.
COPY --from=uv /uv /uvx /usr/local/bin/
RUN uv --version
