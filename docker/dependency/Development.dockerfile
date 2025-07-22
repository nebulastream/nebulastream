# syntax=docker/dockerfile:1
# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
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
        expect \
        bats \
        openjdk-21-jre-headless

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
