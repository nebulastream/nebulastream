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

if ! [ -f "/nebulastream/CMakeLists.txt" ]; then
  echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]"
  exit 1
fi

# Build NES
mkdir -p /nebulastream/build
cd /nebulastream/build
cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_BUILD_PLUGIN_TENSOR_FLOW=1 -DNES_USE_S2=1 ..

make fix-guards
git status
clean=$(git status | grep "nothing to commit (working directory clean)")
if [ -z "$clean" ]; then
    echo "Please run fix-guards target locally before shipping your changes on remote"
    exit 1
else
    echo "No change detected."
    exit 0
fi