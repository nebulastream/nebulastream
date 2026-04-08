#!/bin/bash

set -e

# Configuration
THREADS_NUMBERS=(2 4 8 16 32 64)
QUERIES_FILE="queries.txt"
TIMESTAMP=$(date +%s)
RESULTS_FILE="results_${TIMESTAMP}.json"
POLL_INTERVAL=2  # seconds

# Initialize results file
echo "[]" > "$RESULTS_FILE"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Function to add result to JSON file
add_result() {
    local query_text="$1"
    local query_id="$2"
    local threads="$3"
    local status="$4"
    local started="$5"
    local stopped="$6"
    local error="${7:-}"

    # Create temporary file with new entry
    local temp_file=$(mktemp)

    # Read current results, add new entry, write back
    jq --arg query "$query_text" \
       --arg qid "$query_id" \
       --arg threads "$threads" \
       --arg status "$status" \
       --arg started "$started" \
       --arg stopped "$stopped" \
       --arg error "$error" \
       '. += [{
           query: $query,
           query_id: $qid,
           threads_number: ($threads | tonumber),
           status: $status,
           started: $started,
           stopped: $stopped,
           error: $error
       }]' "$RESULTS_FILE" > "$temp_file"

    mv "$temp_file" "$RESULTS_FILE"
}

# Function to poll query status
poll_query_status() {
    local query_id="$1"

    log "Polling status for query $query_id..." >&2

    while true; do
        # Get query status
        local status_output=$(./query_status.sh)

        # Check if our query_id exists and has stopped
        local query_info=$(echo "$status_output" | jq ".[] | select(.local_query_id == $query_id)")

        if [ -z "$query_info" ]; then
            log "Query $query_id not found in status, waiting..." >&2
            sleep "$POLL_INTERVAL"
            continue
        fi

        # Check if stopped field exists (not null and not empty)
        local stopped=$(echo "$query_info" | jq -r '.stopped')

        if [ "$stopped" != "null" ] && [ -n "$stopped" ]; then
            log "Query $query_id has stopped" >&2

            # Extract all necessary fields
            local status=$(echo "$query_info" | jq -r '.query_status')
            local started=$(echo "$query_info" | jq -r '.started.formatted')
            local stopped_time=$(echo "$query_info" | jq -r '.stopped.formatted')
            local error=$(echo "$query_info" | jq -r '.error // ""')

            echo "$status|$started|$stopped_time|$error"
            return 0
        fi

        log "Query $query_id still running, waiting..." >&2
        sleep "$POLL_INTERVAL"
    done
}

# Main benchmark loop
log "Starting benchmark with timestamp: $TIMESTAMP"

for threads in "${THREADS_NUMBERS[@]}"; do
    log "========================================="
    log "Starting worker with $threads threads"
    log "========================================="

    # Start worker
    worker_container_id=$(./start_worker.sh "$threads")
    log "Worker started with container ID: $worker_container_id"

    # Process each query
    while IFS= read -r query || [ -n "$query" ]; do
        # Skip blank lines
        [ -z "$query" ] && continue

        log "Submitting query: $query"

        # Submit query and capture output
        submit_output=$(./submit_query.sh "$query")
        query_id=$(echo "$submit_output" | tail -n 1)

        log "Query submitted with ID: $query_id"

        # Poll for query completion
        result=$(poll_query_status "$query_id")

        # Parse result
        IFS='|' read -r status started stopped error <<< "$result"

        log "Query $query_id completed with status: $status"

        # Store result
        add_result "$query" "$query_id" "$threads" "$status" "$started" "$stopped" "$error"

    done < "$QUERIES_FILE"

    # Kill worker
    log "Killing worker $worker_container_id"
    ./kill_worker.sh "$worker_container_id"

    log "Completed benchmark for $threads threads"
done

log "========================================="
log "Benchmark complete!"
log "Results saved to: $RESULTS_FILE"
log "========================================="

# Pretty print final results summary
log "Summary:"
jq -r '.[] | "Query ID: \(.query_id) | Threads: \(.threads_number) | Status: \(.status)"' "$RESULTS_FILE"
