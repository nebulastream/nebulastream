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
set -e

if ! [  -f "/nebulastream/CMakeLists.txt" ]; then
  echo "Please mount source code at /nebulastream point. Run [docker run -v <path-to-nes>:/nebulastream -d <nes-image>]"
  exit 1
fi

if [ $# -eq 0 ]
then
    mkdir -p /nebulastream/build
    cd /nebulastream/build

    cmake -DCMAKE_BUILD_TYPE=Release -DNES_SELF_HOSTING=1 -DNES_USE_CCACHE=1 -DNES_USE_OPC=0 -DNES_USE_MQTT=1 -DNES_USE_ADAPTIVE=0 -DNES_BUILD_BENCHMARKS=1 -DNES_LOGGING_LEVEL=FATAL_ERROR ..
    make -j`nproc --ignore=2`

    cd ./nes-benchmark 
    ./e2e-benchmark-main --logPath=logger.log --configPath=../../nes-benchmark/config-examples/E2EConfigs/dailyBenchmarks/filter_one_source.yaml
    #./e2e-benchmark-main --logPath=logger.log --configPath=../../nes-benchmark/config-examples/E2EConfigs/dailyBenchmarks/window_one_source.yaml
    
    ./e2e-benchmark-main --logPath=logger.log --configPath=../../nes-benchmark/config-examples/E2EConfigs/dailyBenchmarks/map_one_source.yaml
    pwd

    timestamp=$(date +%Y%m%d%H%M%S)
    mkdir /nebulastream/results/
    mkdir /nebulastream/results/$timestamp/
    mv *.csv /nebulastream/results/$timestamp/

    result=$?
    rm -rf /nebulastream/build 
    exit $result
else
    exec $@
fi
