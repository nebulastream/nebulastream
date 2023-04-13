#!/bin/bash

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

if [ $# -eq 0 ]
then
  # Start the coordinator.
  nesCoordinator --configPath=/resources/coordinator.yml --coordinatorIp=0.0.0.0 --restIp=0.0.0.0 &
  sleep 2s

  # Start the worker.
  nesWorker --logLevel=LOG_ERROR --physicalSources.type=CSVSource --physicalSources.filePath=/resources/wind-turbine-1.csv --physicalSources.numberOfBuffersToProduce=1024 --physicalSources.physicalSourceName=wind_turbine_1 --physicalSources.logicalSourceName=wind_turbines --physicalSources.skipHeader=true --physicalSources.numberOfTuplesToProducePerBuffer=0 &
  sleep 2s

  # Execute a query.
  curl -d@/resources/rest-query-with-csv-sink.json http://localhost:8081/v1/nes/query/execute-query

  # Sleep for 10 seconds to let the query finish, then stop the container.
  exec sleep 10s
else
    exec $@
fi
