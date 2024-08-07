# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG}

RUN apt update -y \
    && apt install pipx clang-format-${LLVM_VERSION} clang-tidy-${LLVM_VERSION} lldb-${LLVM_VERSION} gdb -y

USER ubuntu
WORKDIR /home/ubuntu
ENV PATH="$PATH:/home/ubuntu/.local/bin"
RUN pipx install guardonce
