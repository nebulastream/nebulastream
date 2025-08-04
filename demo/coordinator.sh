#!/usr/bin/env bash

# Fix for TODO: Trap exit signals to cleanup the query registration
cleanup() {
    /usr/bin/nes-nebuli unregister "${QUERY_ID}"
    exit
}

# Set trap for common exit signals
trap cleanup EXIT INT TERM
QUERY_ID=$(/usr/bin/nes-nebuli register -x -i "$1")

if [ $? -eq 1 ]; then
    echo "Failed to start query"
    exit 1
fi

sleep 1
while true; do
  STATUS=$(/usr/bin/nes-nebuli query "${QUERY_ID}")

  # Check if query is complete or failed
  if echo "${STATUS}" | grep -q "COMPLETE\|FAILED"; then
    echo "Query finished with status: ${STATUS}"
    break
  fi

  sleep 1
done
