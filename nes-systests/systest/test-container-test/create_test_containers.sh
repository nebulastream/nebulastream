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

for var in SYSTEST_IMAGE NES_DIR CONTAINER_WORKDIR TEST_VOLUME TESTCONFIG_VOLUME MQTT_CONFIG_VOLUME; do
  if [ -z "${!var}" ]; then
    echo "ERROR: $var is not set" >&2
    exit 1
  fi
done

# Volume mounts:
#   TESTCONFIG_VOLUME: contains /nes-systests/* -> $NES_DIR (reconstructs $NES_DIR/nes-systests/* for systest discovery)
#   TEST_VOLUME:       test working directory -> $CONTAINER_WORKDIR
#   MQTT_CONFIG_VOLUME: mosquitto.conf and the producer payload -> /mqtt-config
cat <<EOF
services:
  systest:
    image: $SYSTEST_IMAGE
    pull_policy: never
    stop_grace_period: 0s
    command: ["sleep", "infinity"]
    working_dir: $CONTAINER_WORKDIR
    volumes:
      - $TESTCONFIG_VOLUME:$NES_DIR
      - $TEST_VOLUME:$CONTAINER_WORKDIR
    depends_on:
      mqtt_broker:
        condition: service_healthy
      mqtt_producer:
        condition: service_completed_successfully

  mqtt_broker:
    image: eclipse-mosquitto:2.0
    command: ["/usr/sbin/mosquitto", "-c", "/mqtt-config/mosquitto.conf"]
    volumes:
      - $MQTT_CONFIG_VOLUME:/mqtt-config:ro
    healthcheck:
      test: ["CMD", "mosquitto_pub", "-h", "localhost", "-t", "healthcheck", "-m", "test", "-q", "1"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 5s

  mqtt_producer:
    image: alpine:latest
    depends_on:
      mqtt_broker:
        condition: service_healthy
    volumes:
      - $MQTT_CONFIG_VOLUME:/mqtt-config:ro
    # The MQTT broker can only retain one message per topic, so we abuse multiple topics to retain all
    # lines of the input file.
    command: >
      sh -c '
      apk add --no-cache mosquitto-clients > /dev/null 2>&1;
      i=0;
      while IFS= read -r line || [ -n "\$\$line" ]; do
        printf "%s\n" "\$\$line" | mosquitto_pub -h mqtt_broker -t "test/data/\$\$i" -s -r -q 1;
        i=\$\$((i + 1));
      done < /mqtt-config/mqtt-data.jsonl;
      echo "All messages published";
      '

networks:
  default:
    labels:
      nes-test: test-container
volumes:
  $TESTCONFIG_VOLUME:
    external: true
  $TEST_VOLUME:
    external: true
  $MQTT_CONFIG_VOLUME:
    external: true
EOF
