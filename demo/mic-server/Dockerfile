FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      pulseaudio-utils \
      socat \
    && rm -rf /var/lib/apt/lists/*

# create a user with args from Compose
ARG HOST_UID
ARG HOST_GID
RUN (groupadd -g ${HOST_GID} hostgroup \
 && useradd -m -u ${HOST_UID} -g ${HOST_GID} hostuser) || true

USER hostuser
WORKDIR /home/hostuser

CMD ["bash"]
