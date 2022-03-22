#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#quit if command returns non-zero code
set -e

if [ "$ENABLE_PROFILER" == "true" ]
then
  if [ ! -f /usr/bin/perf ]
  then
    # perf needs to be compiled for the current kernel - which depends on the host and may not have a counterpart in the repos of this images distro.
    echo "Profiler enabled but /usr/bin/perf not found. Building perf from source"
    apt-get install -y \
      flex \
      bison \
      libcap-dev \
      systemtap-sdt-dev \
      libiberty-dev \
      libzstd-dev \
    && apt-get clean
    kernel_ver=$(uname --kernel-release | sed 's/-.*//')
    echo "Get perf sources for kernel $kernel_ver"
    cd /opt
    wget --no-verbose https://github.com/gregkh/linux/archive/refs/tags/v"$kernel_ver".tar.gz --output-document=- | zcat > kernel.tar
    wget --no-verbose https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/snapshot/linux-"$kernel_ver".tar.asc --output-document=kernel.tar.asc
    gpg --quiet --locate-keys torvalds@kernel.org gregkh@kernel.org
    echo "Verifying source signature"
    gpg --quiet --verify kernel.tar.asc kernel.tar
    mkdir kernel
    tar -xf kernel.tar -C kernel --strip-components 1
    rm kernel.tar kernel.tar.asc
    make install --directory kernel/tools/perf prefix=/usr/
    rm -rf /opt/kernel
    cd /
    echo "Perf installed for kernel $kernel_ver."
  fi
fi

exec "$@"