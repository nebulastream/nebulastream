# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG}

ARG ANTLR4_VERSION=4.13.2

RUN apt update -y \
    && apt install clang-format-${LLVM_VERSION} clang-tidy-${LLVM_VERSION} lldb-${LLVM_VERSION} gdb jq -y

RUN apt-get update && apt-get install -y \
        default-jre-headless \
        python3              \
        python3-venv         \
        python3-bs4

# The vcpkg port of antlr requires the jar to be available somewhere
ADD --checksum=sha256:eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76 --chmod=744 \
  https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar /opt/antlr-${ANTLR4_VERSION}-complete.jar

RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j\
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version
