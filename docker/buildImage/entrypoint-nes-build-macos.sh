#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

build_dir=$1

# RequireBuild indicates if the build should succeed if we fail during make.
# This is important to check the log to identify build errors on new platforms.
if [ -z "${RequireBuild}" ]; then RequireBuild="true"; else RequireBuild=${RequireBuild}; fi
# RequireTest indicates if the build should succeed if we fail during tests.
# This is important to check the log to identify test errors on new platforms.
if [ -z "${RequireTest}" ]; then RequireTest="false"; else RequireTest=${RequireTest}; fi
echo "Required Build Failed=$RequireBuild"
echo "Required Test Failed=$RequireTest"
echo "Build dir=$build_dir"

# Build NES
# We use ccache to reuse intermediate build files across ci runs.
ccache -s

# configure correct jdk for the specific runner
# we use a local jdk as we cant install different jdk version over brew
if [[ $(uname -m) == 'arm64' ]]; then
  JAVA_HOME="/Users/nesci/jdk-20-arm64/Contents/Home/"
else
  JAVA_HOME="/Users/nesci/jdk-20-x64/Contents/Home/"
fi

export JAVA_HOME="$JAVA_HOME"

cmake --fresh -B "$build_dir/" -DJAVA_HOME="$JAVA_HOME" -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DNES_USE_CCACHE=1 -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_ENGINE=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_JNI=1 -DNES_USE_MQTT=1 -DNES_USE_ADAPTIVE=0 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_BUILD_PLUGIN_TENSOR_FLOW=1 -DNES_USE_S2=1 -DNES_USE_OPENCL=1 .
cmake --build "$build_dir/" -j8
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
