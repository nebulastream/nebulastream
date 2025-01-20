# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG}

ARG ANTLR4_VERSION=4.13.1

RUN apt update -y \
    && apt install clang-format-${LLVM_VERSION} clang-tidy-${LLVM_VERSION} lldb-${LLVM_VERSION} gdb jq -y

# As clang-tidy-diff is not available in the apt repository, we need download it from the github repo and install it
ADD --checksum=sha256:ec28c743ce3354df22b52db59feeac2d98556b8fc81cb7c1a877f3a862ae5726 --chmod=755 \
 https://raw.githubusercontent.com/duckdb/duckdb/52b43b166091c82b3f04bf8af15f0ace18207a64/scripts/clang-tidy-diff.py /usr/bin/

RUN apt-get update && \
        apt-get install -y software-properties-common && \
        add-apt-repository ppa:deadsnakes/ppa && \
        apt-get update && \
        apt-get install -y default-jre-headless python3.11 python3.11-dev python3.11-distutils -y

# Installing the bs4 library to remove the rows from the index.html file
RUN python3.11 -m venv code-cov-venv &&   code-cov-venv/bin/pip install beautifulsoup4 bs4

# The vcpkg port of antlr requires the jar to be available somewhere
ADD --checksum=sha256:bc13a9c57a8dd7d5196888211e5ede657cb64a3ce968608697e4f668251a8487 --chmod=744 \
  https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar /opt/antlr-${ANTLR4_VERSION}-complete.jar

RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j\
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version
