#!/bin/bash

[[ ! -f /nebulastream/CMakeLists.txt ]] && echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]" && exit 1

if [ $# -eq 0 ]
then
    mkdir -p /nebulastream/build
    cd /nebulastream/build
    export CC=/usr/bin/clang
    export CXX=/usr/bin/clang++
    cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-Wall -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " -DNES_USE_OPC=1 -DNES_USE_ADAPTIVE=0 ..
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
