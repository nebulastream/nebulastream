#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs upstream Nexmark queries on Flink (in Docker) over CSV files from ../datagen and writes each
# query's result to <out>/<query>.csv. The query files are the upstream ones at a pinned commit, unless
# queries/<query>.sql exists next to this script (a documented local variant); only the blackhole sink is
# replaced with a CSV filesystem sink.
#
# --mode batch runs Flink's batch runtime: the result is the final state of each query, written insert-only.
# Use it for queries that emit updates (unbounded aggregations, Top-N, regular joins followed by aggregation),
# which the CSV sink cannot consume in streaming mode.
#
# Usage: run_reference.sh [--mode streaming|batch] <data-dir> <out-dir> q0 q1 q2 ...

set -euo pipefail

NEXMARK_COMMIT=6b3646c3baec701f1fa74baf938d235f742e5d3c
FLINK_IMAGE=flink:1.20-java17

MODE=streaming
if [[ "${1:-}" == "--mode" ]]; then
    MODE=$2
    shift 2
fi
[[ "$MODE" == streaming || "$MODE" == batch ]] || { echo "unknown mode: $MODE" >&2; exit 1; }

DATA_DIR=$(realpath "$1")
OUT_DIR=$(realpath -m "$2")
shift 2
SCRIPT_DIR=$(dirname "$(realpath "$0")")

mkdir -p "$OUT_DIR"
WORK_DIR=$(mktemp -d "$OUT_DIR/.work.XXXXXX")
# Kept on failure for the Flink logs in $WORK_DIR/flink-log.
trap 'echo "failed; logs in $WORK_DIR/flink-log" >&2' ERR

for QUERY in "$@"; do
    if [[ -f "$SCRIPT_DIR/queries/$QUERY.sql" ]]; then
        cp "$SCRIPT_DIR/queries/$QUERY.sql" "$WORK_DIR/$QUERY.upstream.sql"
    else
        curl -fsSL "https://raw.githubusercontent.com/nexmark/nexmark/$NEXMARK_COMMIT/nexmark-flink/src/main/resources/queries/$QUERY.sql" \
            -o "$WORK_DIR/$QUERY.upstream.sql"
    fi
    # Swap the blackhole sink for a CSV sink writing into /out/<query>/.
    sed -E "s|'connector' *= *'blackhole'|'connector' = 'filesystem', 'path' = 'file:///out/$QUERY', 'format' = 'csv'|" \
        "$WORK_DIR/$QUERY.upstream.sql" > "$WORK_DIR/$QUERY.body.sql"
    if ! grep -q "file:///out/$QUERY" "$WORK_DIR/$QUERY.body.sql"; then
        echo "$QUERY: no blackhole sink found to replace" >&2
        exit 1
    fi
    # The mode must be set before the DDL: views validated in streaming mode keep a rowtime type that batch rejects.
    { echo "SET 'execution.runtime-mode' = '$MODE';"; cat "$SCRIPT_DIR/ddl_files.sql" "$WORK_DIR/$QUERY.body.sql"; } \
        > "$WORK_DIR/$QUERY.sql"
done

# One container per invocation: start a local cluster, then run each query synchronously (table.dml-sync).
docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$DATA_DIR:/data:ro" -v "$WORK_DIR:/queries:ro" -v "$WORK_DIR:/out" \
    --entrypoint bash "$FLINK_IMAGE" -c '
        set -e
        export FLINK_LOG_DIR=/out/flink-log
        mkdir -p "$FLINK_LOG_DIR"
        # The image config is read-only for a non-root user; extend a copy.
        export FLINK_CONF_DIR=/tmp/flink-conf
        cp -r /opt/flink/conf "$FLINK_CONF_DIR"
        printf "%s\n" "taskmanager.memory.process.size: 8g" "jobmanager.memory.process.size: 2g" \
            >> "$FLINK_CONF_DIR/config.yaml"
        /opt/flink/bin/start-cluster.sh >/dev/null
        for QUERY in '"$*"'; do
            echo "== $QUERY"
            LOG="$FLINK_LOG_DIR/$QUERY.client.log"
            /opt/flink/bin/sql-client.sh -f "/queries/$QUERY.sql" > "$LOG" 2>&1 || { cat "$LOG"; exit 1; }
            if grep -q "ERROR" "$LOG"; then cat "$LOG"; exit 1; fi
        done
    '

for QUERY in "$@"; do
    # Committed part files only; in-progress files start with a dot.
    find "$WORK_DIR/$QUERY" -type f -name 'part-*' -exec cat {} + | sort > "$OUT_DIR/$QUERY.csv"
    echo "$QUERY: $(wc -l < "$OUT_DIR/$QUERY.csv") rows"
done
rm -rf "$WORK_DIR"
