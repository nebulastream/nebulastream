FROM ubuntu:24.04

# 1) Copy & run your install script
COPY scripts/install_local_docker_environment.sh /usr/local/bin/install_script.sh
RUN chmod +x /usr/local/bin/install_script.sh \
 && /usr/local/bin/install_script.sh

# 2) Install Ninja in-container
RUN apt update -y \
 && apt install -y ninja-build

# 3) Default CMake generator
ENV CMAKE_GENERATOR=Ninja
