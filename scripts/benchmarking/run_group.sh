#!/bin/bash

GROUP=$1
REPS=$2

for i in $(seq 1 $REPS);
do
  docker run \
  --rm \
  -u $(id -u):$(id -g) \
  -v "$(pwd)":/tmp/nebulastream \
  -w /tmp/nebulastream/cmake-build-release-docker/nes-systests/systest \
  nebulastream/nes-development:local \
  /tmp/nebulastream/cmake-build-release-docker/nes-systests/systest/systest \
     -g $GROUP \
     -n 1;
done;
