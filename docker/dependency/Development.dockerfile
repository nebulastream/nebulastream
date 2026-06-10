# syntax=docker/dockerfile:1
# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG} AS development-base

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
    cat << 'EOF' > /etc/gdb/gdbinit
python
import sys
sys.path.insert(0, '/usr/share/libcxx')
import printers
printers.register_libcxx_printer_loader()
end
set auto-load safe-path /
EOF

# Same libc++ pretty printer for udb (UndoDB time-travel debugger)
RUN cat << 'EOF' > /root/.udbinit
python
import sys
sys.path.insert(0, '/usr/share/libcxx')
import printers
printers.register_libcxx_printer_loader()
end
set auto-load safe-path /
EOF

# Installing the stable and nightly rust toolchain
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.97.1

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
    rustup component add rustfmt rust-src --toolchain nightly; \
    rustup --version; \
    cargo --version; \
    rustc --version;

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

# Vendor the dependencies of both the NES workspace and nightly's standard library. The latter
# keeps sanitizer builds using `-Zbuild-std` offline as well. The build context is limited to
# Cargo files and Rust sources by Development.dockerfile.dockerignore.
FROM development-base AS cargo-builder
COPY . /tmp/cargo-build/
RUN set -eux; \
    cd /tmp/cargo-build; \
    mkdir -p /opt/nes; \
    NIGHTLY_SYSROOT=$(rustc +nightly --print sysroot); \
    cargo +nightly vendor \
        --locked \
        --versioned-dirs \
        --sync "${NIGHTLY_SYSROOT}/lib/rustlib/src/rust/library/Cargo.toml" \
        /opt/nes/cargo-vendor \
        > /opt/nes/cargo-vendor-config.toml; \
    printf '\n[net]\noffline = true\n' >> /opt/nes/cargo-vendor-config.toml; \
    CXX_VERSION=$(awk '/^name = "cxx"$/{f=1; next} f && /^version = /{gsub(/"/,"",$3); print $3; exit}' Cargo.lock); \
    test -n "$CXX_VERSION"; \
    cargo install cxxbridge-cmd \
        --version "${CXX_VERSION}" \
        --locked \
        --root /opt/nes/cxxbridge; \
    chmod -R a+rX /opt/nes/cargo-vendor /opt/nes/cxxbridge; \
    chmod a+r /opt/nes/cargo-vendor-config.toml

FROM development-base
ENV NES_CARGO_VENDOR_CONFIG=${CARGO_HOME}/config.toml

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

COPY --from=cargo-builder /opt/nes/cargo-vendor /opt/nes/cargo-vendor
COPY --from=cargo-builder /opt/nes/cargo-vendor-config.toml ${NES_CARGO_VENDOR_CONFIG}
COPY --from=cargo-builder /opt/nes/cxxbridge/bin/cxxbridge ${CARGO_HOME}/bin/cxxbridge
