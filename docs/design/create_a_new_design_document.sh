#!/bin/bash

# Check if a title is provided
if [ -z "$1" ]; then
  echo "Usage: $0 \"Specify a title for the new design document.\""
  exit 1
fi

# Get the current date in YYYYMMDD format
current_date=$(date +%Y%m%d)

# Capture all arguments as the title
title="$*"

# Replace spaces in the title with underscores
formatted_title=$(echo "$title" | tr ' ' '_')

# Create the new filename
new_filename="${current_date}_${formatted_title}.md"

# Copy the template to the new file
cp 00000000_template.md "$new_filename"

# Provide feedback to the user
echo "Created new design document with name $new_filename from template."
