#!/bin/bash
set -e

export YAML_FILE=nebulastream/demo/model_input.yaml

docker-compose -f demo/docker-compose.yaml down --volumes --remove-orphans
docker container prune -f
docker image prune -f

echo "Start recording audio..."
# Run in foreground to see logs, but with timeout
timeout 15 docker-compose -f demo/docker-compose.yaml up || true

echo "Stop recording..."
docker-compose -f demo/docker-compose.yaml down