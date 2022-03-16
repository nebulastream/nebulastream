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
  # Start the UI first, so that the user can immediately connect to it.
  cd /app && npm start &
  sleep 20s
  cd /

  # Start the MQTT server
  mosquitto -c /app/mosquitto/mosquitto.conf &

  # Start the coordinator.
  nesCoordinator --configPath=coordinator.yaml &
  sleep 2s

  # Start two workers.
  nesWorker --configPath=worker-1.yaml &
  #nesWorker --configPath=worker-2.yaml &
  sleep 2s

  # Execute a query.
  curl -d@/window-query-mqtt.txt http://localhost:8081/v1/nes/query/execute-query

  # Start bash to keep the docker container running
  /bin/bash
else
    exec $@
fi
