#!/bin/bash

[[ ! -f /iotdb/CMakeLists.txt ]] && echo "Please mount source code at /iotdb point. Run [docker run -v <path-to-iotdb>:/iotdb -d <iotdb-image>]" && exit 1

if [ $# -eq 0 ]
then
    mkdir -p /iotdb/build
    cd /iotdb/build
    cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" ..
    make -j4 && make test
else
    exec $@
fi
