#!/usr/bin/env bash
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
