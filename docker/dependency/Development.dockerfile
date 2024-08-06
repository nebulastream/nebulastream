ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG}

RUN    apt update -y \
    && apt install pip clang-format-${LLVM_VERSION} lldb-${LLVM_VERSION} -y \
    && python3 -m pip install guardonce

ARG USERNAME=nes-developer
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME