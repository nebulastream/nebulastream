#!/bin/bash

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
