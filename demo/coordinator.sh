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

cleanup() {
  /usr/bin/nes-nebuli unregister "${QUERY_ID}"
  exit
}

# Set trap for common exit signals
trap cleanup EXIT INT TERM

if [ $? -eq 1 ]; then
  echo "Failed to start query"
  exit 1
fi

sleep 1
while true; do
  STATUS=$(/usr/bin/nes-nebuli -t ${TOPOLOGY} status "${QUERY_ID}")

  # Check if query is complete or failed
  if echo "${STATUS}" | grep -q "COMPLETE\|FAILED"; then
    echo "Query finished with status: ${STATUS}"
    break
  fi

  sleep 1
done
