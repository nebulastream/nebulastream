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

# RequireBuild indicates if the build should succeed if we fail during make.
# This is important to check the log to identify build errors on new platforms.
if [ -z "${RequireBuild}" ]; then RequireBuild="true"; else RequireBuild=${RequireBuild}; fi
# RequireTest indicates if the build should succeed if we fail during tests.
# This is important to check the log to identify test errors on new platforms.
if [ -z "${RequireTest}" ]; then RequireTest="true"; else RequireTest=${RequireTest}; fi
echo "Required Build Failed=$RequireBuild"
echo "Required Test Failed=$RequireTest"
if [ $# -eq 0 ]
then
    # Build NES
    mkdir -p build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_USE_ADAPTIVE=0 ..
    make -j4
    # Check if build was successful
    errorCode=$?
    if [ $errorCode -ne 0 ];
    then
      rm -rf ./*
      if [ "$RequireBuild" == "true" ];
      then
        echo "Required Build Failed"     
        exit $errorCode
      else
        echo "Optional Build Failed"
        exit 0
      fi
    else
      cd ./tests
      ln -s ../nesCoordinator .
      ln -s ../nesWorker .
      # If build was successful we execute the tests
      # timeout after 70 minutes
      # We don't want to rely on the github-action timeout, because
      # this would fail the job in any case.
      timeout 70m make test_debug
      errorCode=$?
      if [ $errorCode -ne 0 ];
      then
        cd ..
        rm -rf ./build
        if [ "$RequireTest" == "true" ];
        then
          echo "Required Tests Failed"          
          exit $errorCode
        else
          echo "Optional Tests Failed"
          exit 0
        fi
      fi
    fi
else
    exec $@
fi
