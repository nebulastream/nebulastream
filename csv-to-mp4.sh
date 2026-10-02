#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

input=${1:-/home/ls/dima/nebulastream/cmake-build-debug/nes-single-node-worker/rgb.csv}
output=${2:-rgb.mp4}
fps=${3:-23}

frames=$(mktemp -d)
trap 'rm -rf "$frames"' EXIT

i=0
tail -n +2 "$input" | while IFS= read -r base64_jpeg; do
    [[ -z "$base64_jpeg" ]] && continue
    printf '%s' "$base64_jpeg" | base64 --decode >"$frames/frame-$(printf '%06d' "$i").jpg"
    ((i += 1))
done

ffmpeg -y -framerate "$fps" -i "$frames/frame-%06d.jpg" \
    -c:v libx264 -pix_fmt yuv420p "$output"
