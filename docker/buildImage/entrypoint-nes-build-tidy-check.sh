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

# We expect the diff to be located at /clang-tidy-result/git_pr.diff
GIT_DIFF_FILE_NAME="/clang-tidy-result/git_pr.diff"


# generate buildsystem
mkdir -p /nebulastream/build
cd /nebulastream/build
CLANG_TIDY_EXECUTABLE=$(cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_BUILD_PLUGIN_TENSOR_FLOW=1 -DNES_USE_S2=1 .. 2>&1 | grep "$CLANG_TIDY_EXECUTABLE" | cut -d '=' -f2)
echo "CLANG_TIDY_EXECUTABLE: $CLANG_TIDY_EXECUTABLE"


# For the script to work, we need to install pyyaml and for that we need pip
apt-get update
apt-get install -y python3-pip
pip install pyyaml

# run clang-tidy and pass the contents of the file to the script via stdin
cat $GIT_DIFF_FILE_NAME | python3 /nebulastream/scripts/build/run_clang_tidy_diff.py -clang-tidy-binary "$CLANG_TIDY_EXECUTABLE" -p1 -j 4 -path /nebulastream/build -export-fixes /clang-tidy-result/fixes.yml
