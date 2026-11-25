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

# Runs the batches from the PR #2145 review, inside the dev container. About 3 h in total.
# Usage: matrix.sh <out-dir> [batch...]; batches: low low_q high twonode mqtt_high low_inflight8
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
OUT=${1:?usage: matrix.sh <out-dir> [batch...]}
shift
BATCHES=${*:-low low_q high twonode mqtt_high low_inflight8}
LOW=(--rate 1000 --keys 10000)
HIGH=(--rate 20000 --keys 400000 --worker-arg=--worker.total_memory_in_bytes=4294967296)
TWO_NODE=$REPO/nes-systests/configs/topologies/two-node.yaml

for batch in $BATCHES; do
  case $batch in
    low) python3 "$HERE/bench.py" --out "$OUT/low" "${LOW[@]}" --scenarios steady delay500ms delay20s jitter3s jitterfast sine10s \
      batch5s outage20s late20s idle20s replay30s sparse file_live ;;
    low_q) python3 "$HERE/bench.py" --out "$OUT/low_q" "${LOW[@]}" --scenarios j3_steady j3_cdelay20s j3_adelay20s u8_steady \
      u8_onedelay20s u8_jitter3s ;;
    high) python3 "$HERE/bench.py" --out "$OUT/high" "${HIGH[@]}" --scenarios steady delay20s jitter3s outage20s late20s replay30s \
      file_live ;;
    twonode) python3 "$HERE/bench.py" --out "$OUT/twonode" "${LOW[@]}" --topology "$TWO_NODE" --scenarios steady delay20s jitter3s \
      outage20s file_live ;;
    mqtt_high) python3 "$HERE/bench.py" --out "$OUT/mqtt_high" "${HIGH[@]}" --transport mqtt --gaps 0 1000 --scenarios mqtt_steady \
      mqtt_delay20s mqtt_outage20s mqtt_late20s ;;
    low_inflight8) python3 "$HERE/bench.py" --out "$OUT/low_inflight8" "${LOW[@]}" --gaps 0 1000 --scenarios delay20s outage20s \
      --worker-arg=--worker.default_max_inflight_buffers=8 ;;
    *) echo "unknown batch $batch" >&2; exit 1 ;;
  esac
  python3 "$HERE/summarize.py" "$OUT/$batch/results.jsonl"
done
