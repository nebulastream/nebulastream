#!/usr/bin/env bats

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source "$NES_BATS_LIB"

setup_file()    { nes_distributed_setup_file "$NES_CLI" video-source; }
teardown_file() { nes_distributed_teardown_file; }
setup()         { nes_distributed_setup; }
teardown()      { nes_distributed_teardown; }

# Frames the sink wrote so far (the first line of results.csv is the header).
frames() { echo $(($(wc -l < worker-1/results.csv) - 1)); }
# Usage: wait_for_frames <count>: waits until the sink wrote more than <count> frames.
wait_for_frames() {
  local deadline=$((SECONDS + 30))
  until [ -f worker-1/results.csv ] && [ "$(frames)" -gt "$1" ]; do
    if ((SECONDS >= deadline)); then
      fail "timed out waiting for more than $1 frames"
      return 1
    fi
    sleep 0.5
  done
}

@test "video source delivers the camera's frames" {
  setup_distributed tests/good/video-to-file.yaml
  run docker_nes_cli -t tests/good/video-to-file.yaml start
  [ "$status" -eq 0 ]

  wait_for_frames 25
  # The fake camera's 512x512 Mono8 frames (pixel format 0x01080001)
  grep -q ',512,512,17301505$' worker-1/results.csv
}

@test "video source connects to the camera again when its stream stops" {
  setup_distributed tests/good/video-to-file.yaml
  run docker_nes_cli -t tests/good/video-to-file.yaml start
  [ "$status" -eq 0 ]
  wait_for_frames 25

  # A paused camera sends nothing and misses the source's heartbeat, as a camera that ends the stream on its own does.
  # The pause is longer than the source's stall timeout of 5 seconds.
  docker compose pause fake-camera
  sleep 8
  docker compose unpause fake-camera

  local before
  before=$(frames)
  wait_for_frames $((before + 25))
  grep -q "connecting to the camera again" worker-1/singleNodeWorker.log
}
