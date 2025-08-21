#!/bin/bash

# Check if N is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_instances>"
  exit 1
fi

N=$1
LIVE_RECORD_EXEC="$(pwd)/build-asan/nes-nebuli/nes-nebuli-embedded"
QUERY_CONFIG="-i $(pwd)/test.sql"

# Create a main temporary directory for all runs
MAIN_TMP_DIR=$(mktemp -d -t live-record-parallel-XXXXXX)
echo "Using main temporary directory: $MAIN_TMP_DIR"

echo "Starting $N instances of live-record in parallel..."

for i in $(seq 1 $N); do
  INSTANCE_TMP_DIR=$(mktemp -d -p "$MAIN_TMP_DIR" live-record-instance-XXXXXX)
  echo "Starting live-record instance $i in $INSTANCE_TMP_DIR"
  (
    # Change to the instance-specific temporary directory
    cd "$INSTANCE_TMP_DIR" || { echo "Error: Could not change directory to $INSTANCE_TMP_DIR" >&2; exit 1; }
    # Run live-record in the background
    ASAN_OPTIONS="detect_leaks=0" live-record --tmpdir-root=/data-ssd/lukas/tmp --thread-fuzzing --disable-aslr --save-on error --retry-for 1h "$LIVE_RECORD_EXEC" $QUERY_CONFIG &
    echo "Process PID: $!"
  ) &
done

echo "All $N live-record instances have been started in the background."
echo "They are running in subdirectories of: $MAIN_TMP_DIR"
echo "You will need to manually stop these processes (e.g., using 'kill <PID>') and delete the temporary directories when done."
