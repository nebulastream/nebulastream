# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG}

RUN apt update -y \
    && apt install clang-format-${LLVM_VERSION} clang-tidy-${LLVM_VERSION} lldb-${LLVM_VERSION} gdb jq -y

# As clang-tidy-diff is not available in the apt repository, we need download it from the github repo and install it
ADD --checksum=sha256:ec28c743ce3354df22b52db59feeac2d98556b8fc81cb7c1a877f3a862ae5726\
 https://raw.githubusercontent.com/duckdb/duckdb/52b43b166091c82b3f04bf8af15f0ace18207a64/scripts/clang-tidy-diff.py /clang-tidy-diff.py

RUN apt-get update && \
        apt-get install -y software-properties-common && \
        add-apt-repository ppa:deadsnakes/ppa && \
        apt-get update && \
        apt-get install -y python3.11 python3.11-dev python3.11-distutils -y

RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j\
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version

USER ubuntu
WORKDIR /home/ubuntu
