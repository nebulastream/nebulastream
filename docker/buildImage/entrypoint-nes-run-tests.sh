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

# RequireTest indicates if the build should succeed if we fail during tests.
# This is important to check the log to identify test errors on new platforms.
if [ -z "${RequireTest}" ]; then RequireTest="true"; else RequireTest=${RequireTest}; fi
echo "Required Test Failed=$RequireTest"
if [ $# -eq 1 ]; then
  # If build was successful we execute the tests
  # timeout after 90 minutes
  # We don't want to rely on the github-action timeout, because
  # this would fail the job in any case.
  cd /build_dir
  # Select which test to run based in the argument of this entrypoint
  if [ "$1" = "gpu" ]; then
    timeout 90m make test_gpu
  elif [ "$1" = "default" ]; then
    timeout 90m make test_default
  else
    echo "Invalid argument. Known argument: 'gpu' to build test_gpu or 'default' to build test_default."
    exit 1
  fi
  errorCode=$?
  if [ $errorCode -ne 0 ]; then
    rm -rf /nebulastream/build
    if [ "$RequireTest" = "true" ]; then
      echo "Required Tests Failed"
      exit $errorCode
    else
      echo "Optional Tests Failed"
      exit 0
    fi
  fi
else
  exec $@
fi
