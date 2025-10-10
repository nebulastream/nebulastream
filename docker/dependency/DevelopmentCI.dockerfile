# The Development CI image installs a github runner specific user
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

# The UID env var should be used in child Containerfile.
ENV UID=1001
ENV GID=1001
ENV USERNAME="runner"
USER root
RUN groupadd --gid ${GID} runner && useradd $USERNAME --gid ${GID} --uid ${UID}
RUN mkdir -p /home/${USERNAME} \
    && chown -R $USERNAME:$GID /home/runner

WORKDIR /home/runner
ENV HOME=/home/${USERNAME}
ENV MOLD_JOBS=1
USER 1001:1001

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/${USERNAME}/.cargo/bin:${PATH}"
