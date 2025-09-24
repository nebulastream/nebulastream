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

DOCKER_COMPOSE_FILE="docker-compose.monitoring.yaml"
DATA_SCRIPT="./generate_data/sys_resources.sh"

if [ -z "$1" ]; then
  echo "Usage: $0 <input_yaml_filename>"
  exit 1
fi

INPUT_FILENAME="$1"
BASENAME=$(basename "$INPUT_FILENAME")
CSV_NAME="${BASENAME%.*}.csv"  # .yaml → .csv

if [ ! -f "$INPUT_FILENAME" ]; then
  echo "File not found: $INPUT_FILENAME"
  exit 1
fi

# Patch compose file
TMP_COMPOSE_FILE="docker-compose.monitoring.generated.yml"

sed -E "
  s@(register -i )/app/[^ ]+@\1/app/$BASENAME@;
  s@(/tmp/)<query_file_name>.csv@\1${CSV_NAME}@
" ${DOCKER_COMPOSE_FILE} > ${TMP_COMPOSE_FILE}


docker compose -f "$TMP_COMPOSE_FILE" down
docker compose -f "$TMP_COMPOSE_FILE" pull
docker compose -f "$TMP_COMPOSE_FILE" up &
COMPOSE_PID=$!

cleanup() {
  docker compose -f "$TMP_COMPOSE_FILE" down
  rm -f "$TMP_COMPOSE_FILE"
}
trap cleanup EXIT

#bash "$DATA_SCRIPT"

wait
