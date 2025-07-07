#!/bin/bash
set -e

STEP=${1:-1}
export YAML_FILE=nebulastream/demo/word_detection_${STEP}.yaml

docker-compose -f demo/docker-compose.yaml down --volumes --remove-orphans
docker container prune -f
docker image prune -f

echo "Start recording audio..."
# Run in foreground to see logs, but with timeout
timeout 30 docker-compose -f demo/docker-compose.yaml up || true

echo "Stop recording..."
docker-compose -f demo/docker-compose.yaml down