#!/bin/bash

mkdir -p /IoTDB/iotdb/build
cd /IoTDB/iotdb/build
cmake -DCMAKE_BUILD_TYPE=Release -DGRPC_AVAILABLE=1 -DZMQ_AVAILABLE=1 -DCPPZMQ_AVAILABLE=1 -DNES_BUILDING_ON_CI=1 -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-w -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " ..
make release
