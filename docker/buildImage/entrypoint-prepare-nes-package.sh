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

# We expect the number of threads to be passed as an argument
num_threads=$1


# Build NES
python3 /nebulastream/scripts/build/check_license.py /nebulastream /nebulastream/.no-license-check || exit 1

# Depending if the ccache_dir exists, we will instruct NebulaStream to compile with a ccache in mind or not
USE_CCACHE=0
if [ -d /ccache_dir ]
then
  # Compiling NebulaStream with ccache
  echo "Compiling NebulaStream with ccache..."
  ccache --set-config=cache_dir=/cache_dir/
  ccache -M 10G
  USE_CCACHE=1
fi

cmake --fresh -B /build_dir -DCMAKE_BUILD_TYPE=Release -DNES_USE_CCACHE=$USE_CCACHE -DNES_SELF_HOSTING=1 -DNES_ENABLES_TESTS=1 -DNES_USE_OPC=0 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_ENGINE=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR=1 -DNES_USE_KAFKA=1 -DNES_USE_MQTT=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_JNI=1 -DNES_JAVA_UDF_UTILS_PATH=lib/NebulaStream/Java -DNES_BUILD_PLUGIN_TENSORFLOW=1 -DNES_BUILD_PLUGIN_ARROW=1 -DNES_USE_ADAPTIVE=0 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_USE_S2=1 /nebulastream/
cmake --build /build_dir -j $num_threads


cd /build_dir
rm *deb
cpack
