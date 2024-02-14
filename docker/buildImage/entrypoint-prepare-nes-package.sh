#!/bin/sh

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
#set -e

if ! [  -f "/nebulastream/CMakeLists.txt" ]; then
  echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]"
  exit 1
fi

if [ $# -eq 0 ]
then
    # Build NES
    python3 /nebulastream/scripts/build/check_license.py /nebulastream || exit 1
    ccache --set-config=cache_dir=/cache_dir/
    ccache -M 10G
    cmake --fresh -B /build_dir -DCMAKE_BUILD_TYPE=Release -DNES_USE_CCACHE=1 -DNES_SELF_HOSTING=1 -DNES_ENABLES_TESTS=1 -DNES_USE_OPC=0 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_ENGINE=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR=1 -DNES_USE_KAFKA=1 -DNES_USE_MQTT=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_JNI=1 -DNES_JAVA_UDF_UTILS_PATH=lib/NebulaStream/Java -DNES_BUILD_PLUGIN_TENSORFLOW=1 -DNES_BUILD_PLUGIN_ARROW=1 -DNES_USE_ADAPTIVE=0 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_USE_S2=1 -DNES_USE_OPENCL=1 -DNES_USE_JNI_OPENCV=1 -DNES_ELEGANT_USE_DUMMY_KERNEL=1 -DNES_BUILD_PLUGIN_ARROW=1 -DNES_ELEGANT_DECOMPILE_JAVA_UDF_CLASSES=1 -DNES_ELEGANT_DECOMPILER_PATH=lib/NebulaStream/Java /nebulastream/
    cmake --build /build_dir -j12
    cd /build_dir
    rm *deb
    cpack
else
    exec $@
fi
