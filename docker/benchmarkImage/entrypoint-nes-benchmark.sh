#!/bin/bash

[[ ! -f /nebulastream/CMakeLists.txt ]] && echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]" && exit 1

if [ $# -eq 0 ]
then
    mkdir -p /nebulastream/build
    cd /nebulastream/build
    export CC=/usr/bin/clang
    export CXX=/usr/bin/clang++
    cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-w -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " -DNES_USE_OPC=1 -DNES_USE_ADAPTIVE=0 -DNES_BUILD_BENCHMARKS=1 ..
    make -j`nproc --ignore=2`
    cd /nebulastream/benchmark/scripts
    python3 run_and_plot_benchmarks.py -f ../../build/benchmark/ -nc
    result=$?
    rm -rf /nebulastream/build
    exit $result
else
    exec $@
fi
