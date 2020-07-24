#!/bin/bash

[[ ! -f /nebulastream/CMakeLists.txt ]] && echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]" && exit 1

if [ $# -eq 0 ]
then
    mkdir -p /nebulastream/build
    cd /nebulastream/build
    export CC=/usr/bin/clang
    export CXX=/usr/bin/clang++
    if test -f *.deb; then
        rm *.deb
    fi
    cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING=" -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " ..
    make -j4
    cpack
    cd ..
    if test -f *.deb; then 
        rm docker/executableImage/resources/*.deb
    fi
    cp build/*.deb docker/executableImage/resources
    result=$?
    rm -rf /nebulastream/build
    exit $result
else
    exec $@
fi
