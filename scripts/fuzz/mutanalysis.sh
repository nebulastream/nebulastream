#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

export CCACHE_DIR=/ccache

git clone --depth=1 --branch=fuzz https://github.com/nebulastream/nebulastream.git
cd nebulastream || exit 1

python3 standalone_mutator.py -o mutations $(git ls-files -- "*.cpp" | grep -v test | grep -v fuzz)

export AFL_SKIP_CPUFREQ=1
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1

cmake -B cmake-build-nrm -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
# CC=afl-clang-lto CXX=afl-clang-lto++ cmake -B cmake-build-afl -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
# CC=hfuzz-clang   CXX=hfuzz-clang++   cmake -B cmake-build-hfz -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF

cmake --build cmake-build-nrm
# cmake --build cmake-build-afl
# cmake --build cmake-build-hfz

for patch in $(find mutations -name "*.patch" -print0 | xargs -0 sha256sum | sort | awk '{ print $2 }')
do
    if ! python3 check_mutations.py $patch cmake-build-nrm/compile_commands.json || ! cmake --build cmake-build-nrm
    then
        echo cannot build $patch
        continue
    fi

    if ! ctest -j 32 --test-dir cmake-build-nrm --stop-on-failure
    then
        echo mutanth $patch killed by ctest
    fi
done
