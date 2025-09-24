#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BUFFER_FILE="/tmp/keystrokes_buffer.csv"

# Clear the buffer file at the start
> "$BUFFER_FILE"

# Open file descriptor 3 for reading keystrokes
exec 3< /dev/tty

echo "Starting keystroke capture. Press Ctrl+C to stop."

# Read keystrokes one by one
while IFS= read -r -n1 key <&3; do
  # Get current timestamp in milliseconds
  ts=$(($(date +%s)*1000))

  # Format line without embedded newline
  line="${key},${ts}"

  # Append to buffer with newline
  printf '%s\n' "$line" >> "$BUFFER_FILE"

  # Debug statement to print captured keystrokes
  echo "Debug: $line"
done

# Close file descriptor 3
exec 3<&-

echo "Keystroke capture stopped. Data saved to $BUFFER_FILE."
