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
# parallel test
if [ -z "${NesTestParallelism}" ]; then NesTestParallelism="1"; else NesTestParallelism=${NesTestParallelism}; fi
if [ -z "${NesBuildParallelism}" ]; then NesBuildParallelism="8"; else NesBuildParallelism=${NesBuildParallelism}; fi
echo "Required Build Failed=$RequireBuild"
if [ $# -eq 0 ]; then
  # Build NES
  python3 /nebulastream/scripts/build/check_license.py /nebulastream || exit 1
  # We use ccache to reuse intermediate build files across ci runs.
  # All cached data is stored at /tmp/$os_$arch
  ccache --set-config=cache_dir=/cache_dir/
  ccache -M 10G
  ccache -s
  cmake --fresh -B /build_dir -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DNES_SELF_HOSTING=1 -DNES_USE_CCACHE=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_JNI=1 -DNES_TEST_PARALLELISM=$NesTestParallelism -DNES_USE_TF=1 -DNES_USE_S2=1 -DNES_BUILD_PLUGIN_TENSORFLOW=1 -DNES_USE_OPENCL=1 /nebulastream/
  cmake --build /build_dir -j$NesBuildParallelism
  # Check if build was successful
  errorCode=$?
  ccache -s
  if [ $errorCode -ne 0 ]; then
    if [ "$RequireBuild" = "true" ]; then
      echo "Required Build Failed"
      exit $errorCode
    else
      echo "Optional Build Failed"
      exit 0
    fi
  fi
  exit 0
else
  exec $@
fi
