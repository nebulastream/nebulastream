# syntax=docker/dockerfile:1
# Development image that installs MEOS from a local tarball in the build context
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
        openjdk-21-jre-headless \
        libgeos-dev \
        libproj-dev \
        libgsl-dev \
        libjson-c-dev \
        autoconf \
        automake \
        libtool \
        pkg-config

# The vcpkg port of antlr requires the jar to be available somewhere
ADD --checksum=sha256:eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76 --chmod=744 \
  https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar /opt/antlr-${ANTLR4_VERSION}-complete.jar

RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j\
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version

# Build and install MEOS from local tarball (required by MEOS plugin)
# Place the tarball at docker/dependency/assets/meos.tar.gz before building.
ADD docker/dependency/assets/meos.tar.gz /tmp/meos.tar.gz
RUN set -eux; \
    cd /tmp; \
    tar -xzf meos.tar.gz; \
    meos_src=$(find . -maxdepth 1 -type d -name 'MEOS-*' | head -n 1); \
    cd "$meos_src"; \
    if [ -f CMakeLists.txt ]; then \
      cmake -B build -S . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
      && cmake --build build -j \
      && cmake --install build; \
    else \
      (./autogen.sh || true) \
      && ./configure --prefix=/usr/local \
      && make -j"$(nproc)" \
      && make install; \
    fi; \
    rm -rf /tmp/meos.tar.gz "$meos_src"

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
