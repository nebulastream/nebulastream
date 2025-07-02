#!/usr/bin/env bash
# install_toolchain.sh — Install LLVM, Mold, CMake on Ubuntu 24.04
# Usage: sudo ./install_toolchain.sh

# set script to fail immediately if one command fails, treat unset vars as errors and don't mask error codes
set -euo pipefail

### Configuration (override by exporting env vars) ###
LLVM_TOOLCHAIN_VERSION="${LLVM_TOOLCHAIN_VERSION:-19}"
MOLD_VERSION="${MOLD_VERSION:-2.37.1}"
CMAKE_VERSION="${CMAKE_VERSION:-3.31.6}"
# Default CMake generator
export CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"

### Ensure running as root ###
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root or via sudo." >&2
  exit 1
fi

echo "=== Updating apt and installing prerequisites ==="
apt update -y
DEBIAN_FRONTEND=noninteractive apt install -y \
    wget zip unzip tar curl gpg git ca-certificates \
    linux-libc-dev build-essential g++-14 make \
    libssl-dev openssl ccache pkg-config

echo "=== Adding LLVM apt repository and installing LLVM ${LLVM_TOOLCHAIN_VERSION} ==="
# Create keyring directory if missing
mkdir -p /etc/apt/keyrings
curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key \
     | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg
chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg

# Add apt sources.list for LLVM
ARCH=$(dpkg --print-architecture)
CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
cat > /etc/apt/sources.list.d/llvm-snapshot.list <<EOF
deb [arch=${ARCH} signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] \
    http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_TOOLCHAIN_VERSION} main
deb-src [arch=${ARCH} signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] \
    http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_TOOLCHAIN_VERSION} main
EOF

apt update -y
DEBIAN_FRONTEND=noninteractive apt install -y \
    clang-${LLVM_TOOLCHAIN_VERSION} \
    libc++-${LLVM_TOOLCHAIN_VERSION}-dev \
    libc++abi-${LLVM_TOOLCHAIN_VERSION}-dev \
    libclang-rt-${LLVM_TOOLCHAIN_VERSION}-dev

echo "=== Installing Mold linker version ${MOLD_VERSION} ==="
TMPDIR=$(mktemp -d)
pushd "$TMPDIR"
wget -q "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz"
tar xf "mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz"
cp -r "mold-${MOLD_VERSION}-$(uname -m)-linux/"* /usr/
popd
rm -rf "$TMPDIR"
echo "Mold version: $(mold --version)"

echo "=== Installing CMake version ${CMAKE_VERSION} ==="
TMPDIR=$(mktemp -d)
pushd "$TMPDIR"
wget -q "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz"
tar xf "cmake-${CMAKE_VERSION}.tar.gz"
cd "cmake-${CMAKE_VERSION}"
./configure --parallel="$(nproc)" --prefix=/usr
make -j"$(nproc)" install
popd
rm -rf "$TMPDIR"
echo "CMake version: $(cmake --version | head -n1)"

echo "=== Configuring Clang as the default compiler ==="
# Symlink cc and c++ to clang
ln -sf /usr/bin/clang-"${LLVM_TOOLCHAIN_VERSION}" /usr/bin/cc
ln -sf /usr/bin/clang++-"${LLVM_TOOLCHAIN_VERSION}" /usr/bin/c++
# Register alternatives
update-alternatives --install /usr/bin/cc cc /usr/bin/clang-"${LLVM_TOOLCHAIN_VERSION}" 30
update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-"${LLVM_TOOLCHAIN_VERSION}" 30
update-alternatives --auto cc
update-alternatives --auto c++

echo "/usr/lib/llvm-${LLVM_TOOLCHAIN_VERSION}/lib" \
     > /etc/ld.so.conf.d/libcxx.conf
ldconfig

echo "Default compiler:"
ls -l /usr/bin/cc /usr/bin/c++
cc --version
c++ --version

echo "=== Installation complete ==="