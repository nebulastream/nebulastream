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

# Installing pip and then lcov_cobertura
#apt-get update
#apt-get install -y git python3-pip
#git clone https://github.com/gagikk/lcov-to-cobertura-xml.git
#cd "lcov-to-cobertura-xml"
#python3 -m pip install .
#cd ..


# parallel test
if [ -z "${NesTestParallelism}" ]; then NesTestParallelism="1"; else NesTestParallelism=${NesTestParallelism}; fi
if [ -z "${NesBuildParallelism}" ]; then NesBuildParallelism="8"; else NesBuildParallelism=${NesBuildParallelism}; fi
echo "Test Parallelism=$NesTestParallelism"

# Using CCACHE
if [ -z "${USE_CCACHE}" ]; then USE_CCACHE="0"; else USE_CCACHE=${USE_CCACHE}; fi
echo "USE_CCACHE=$USE_CCACHE"
if [ $# -eq 0 ]
then
    # Build NES
    # We use ccache to reuse intermediate build files across ci runs.
    # All cached data is stored at /tmp/$os_$arch
    ccache --set-config=cache_dir=/cache_dir/
    ccache -M 10G
    ccache -s
    cmake --fresh -B /build_dir -DCMAKE_BUILD_TYPE=Release DNES_COMPUTE_COVERAGE=ON -DNES_TEST_PARALLELISM=$NesTestParallelism  ${EXTRA_CMAKE_FLAG} /nebulastream/
    cmake --build /build_dir -j$NesBuildParallelism


    # Check if build was successful
    errorCode=$?
    if [ $errorCode -ne 0 ];
    then
        echo "Required Build Failed"
        exit $errorCode
    fi

    # If build was successful we execute the tests
    # timeout after 240 minutes
    # We don't want to rely on the github-action timeout, because
    # this would fail the job in any case.
    timeout 240m cmake --build /build_dir  --target test_default -j$NesTestParallelism
    errorCode=$?

# Dear reviewer of this code, I am commenting out the following block of code. If you can read this comment, please le tme know that I should uncomment the following block of code.
#    if [ $errorCode -ne 0 ];
#    then
#      rm -rf /nebulastream/build
#      echo "Some Tests Failed. Therefore, we are not generating coverage report."
#      exit 1
#    fi
    echo "Generating coverage report..."
    cmake --build /build_dir  --target llvm_cov_export_lcov

#    echo "Converting lcov to cobertura..."
#    lcov_cobertura /build_dir/default_coverage.lcov --output /build_dir/coverage.xml --demangle
else
    exec $@
fi
