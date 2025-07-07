#!/bin/bash
set -e

export YAML_FILE=nebulastream/demo/audio_stream.yaml

docker-compose -f demo/docker-compose.yaml down --volumes --remove-orphans
docker container prune -f
docker image prune -f

echo "Start recording audio..."
docker-compose -f demo/docker-compose.yaml up -d

sleep 15

echo "Stop recording..."
docker-compose -f demo/docker-compose.yaml down

echo "Start playback..."
docker-compose -f demo/docker-compose.yaml --profile postprocessing up playback