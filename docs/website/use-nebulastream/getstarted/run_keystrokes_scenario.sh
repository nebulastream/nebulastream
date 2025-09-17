#!/bin/bash

DOCKER_COMPOSE_FILE="docker-compose.keystrokes.yaml"

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
TMP_COMPOSE_FILE="docker-compose.keystrokes.generated.yml"

sed -E "
  s@(register -i )/app/[^ ]+@\1/app/$BASENAME@;
  s@(/tmp/)<query_file_name>.csv@\1${CSV_NAME}@
" ${DOCKER_COMPOSE_FILE} > ${TMP_COMPOSE_FILE}

docker compose -f "$TMP_COMPOSE_FILE" down --remove-orphans
docker compose -f "$TMP_COMPOSE_FILE" pull
docker compose -f "$TMP_COMPOSE_FILE" up &
COMPOSE_PID=$!

cleanup() {
  docker compose -f "$TMP_COMPOSE_FILE" down
  rm -f "$TMP_COMPOSE_FILE"
}
trap cleanup EXIT

wait