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

if ! [ -f "/nebulastream/CMakeLists.txt" ]; then
  echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]"
  exit 1
fi

# RequireBuild indicates if the build should succeed if we fail during make.
# This is important to check the log to identify build errors on new platforms.
if [ -z "${RequireBuild}" ]; then RequireBuild="true"; else RequireBuild=${RequireBuild}; fi
if [ -z "${NesBuildParallelism}" ]; then NesBuildParallelism="8"; else NesBuildParallelism=${NesBuildParallelism}; fi
echo "Required Build Failed=$RequireBuild"
if [ $# -eq 0 ]; then

  # Create compile_commands.json
  cmake -B /build_dir -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_JNI=1 -DNES_TEST_PARALLELISM=$NesTestParallelism -DNES_USE_S2=1 -DNES_USE_OPENCL=1 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_BUILD_PLUGIN_TENSORFLOW=1 -DNES_BUILD_PLUGIN_ARROW=1 -DCMAKE_EXPORT_COMPILE_COMMANDS=1 /nebulastream

  # Translate project to infer format
  infer/bin/infer capture --compilation-database /build_dir/compile_commands.json --jobs $NesBuildParallelism --keep-going -o /build_dir/infer-out

  # Only analyze changed files
  cd /nebulastream
  git diff --name-only origin/master.. > index.txt
  cd -
  infer/bin/infer analyze --quandary --starvation --siof --racerd --liveness --biabduction --uninit --pulse --bufferoverrun --loop-hoisting --topl --jobs $NesBuildParallelism --changed-files-index /nebulastream/index.txt --keep-going -o /build_dir/infer-out
  exit 0
else
  exec $@
fi
