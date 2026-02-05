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

set -eo pipefail

# Start building the compose file
cat <<EOF
services:
  systest:
    image: systest-image
    pull_policy: never
    stop_grace_period: 0s
    command: ["sleep", "infinity"]
    working_dir: $NES_DIR
    volumes:
      - /tmp:/tmp
      - $NES_DIR:$NES_DIR
    depends_on:
      mqtt_broker:
        condition: service_healthy
      mqtt_producer:
        condition: service_completed_successfully

  mqtt_broker:
    image: eclipse-mosquitto:2.0
    container_name: mqtt_broker_container
    ports:
      - "1883:1883"
      - "9001:9001"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
      - mosquitto_data:/mosquitto/data
    healthcheck:
      test: ["CMD", "mosquitto_pub", "-h", "localhost", "-t", "healthcheck", "-m", "test", "-q", "1"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 5s

  mqtt_producer:
    image: alpine:latest
    container_name: mqtt_producer_container
    depends_on:
      mqtt_broker:
        condition: service_healthy
    volumes:
      - ./mqtt-data.jsonl:/mqtt-data.jsonl:ro
    command: >
      sh -c '
      apk add --no-cache mosquitto-clients > /dev/null 2>&1;
      i=0;
      while IFS= read -r line || [ -n "\$\$line" ]; do
        printf "%s\n" "\$\$line" | mosquitto_pub -h mqtt_broker -t "test/data/\$\$i" -s -r -q 1;
        i=\$\$((i + 1));
      done < /mqtt-data.jsonl;
      echo "All messages published";
      '

volumes:
  mosquitto_data:


EOF
