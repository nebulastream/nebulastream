#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



[[ ! -f /nebulastream/CMakeLists.txt ]] && echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]" && exit 1

if [ $# -eq 0 ]
then
    mkdir -p /nebulastream/build
    cd /nebulastream/build
    export CC=/usr/bin/clang
    export CXX=/usr/bin/clang++
    python3 /nebulastream/scripts/build/check-license.py /nebulastream || exit 1
    cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-w -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " -DNES_USE_OPC=1 -DNES_USE_ADAPTIVE=0 ..
    make -j4
    cd /nebulastream/build/tests
    ln -s ../nesCoordinator .
   	ln -s ../nesWorker .
    make test_debug
    result=$?
    rm -rf /nebulastream/build
    exit $result
else
    exec $@
fi
