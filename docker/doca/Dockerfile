FROM nvcr.io/nvidia/doca/doca:2.6.0-devel

# install a debugger and the second CLion-supported build system
RUN DEBIAN_FRONTEND="noninteractive" apt-get update -qq && apt-get install -qq \
  gdb \
  ninja-build \
  lsb-release \
  libdwarf-dev \
  libdwarf1 \
  binutils-dev \
  libunwind-dev \
  libdw-dev \
  libssl-dev \
  build-essential \
  clang-format \
  libnuma-dev \
  numactl \
  libmbedtls-dev \
  libjemalloc-dev \
  git \
  wget \
  python3 \
  python3-dev \
  libsodium-dev \
  tar \
  p7zip \
  doxygen \
  graphviz \
  libbsd-dev \
  software-properties-common \
  ccache \
&& apt-get clean -qq