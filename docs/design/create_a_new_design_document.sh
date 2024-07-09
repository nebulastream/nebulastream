#!/bin/bash

# This script takes a mandatory design document title.
# It interprets all arguments as the title string, so quotes are not required.
# It then creates a new file using the following schema: `YYYYMMDD_NAME_OF_THE_DESIGN_DOCUMENT.md`
# Finally, it copies the content of the design document template to the new file.

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

# Create new file and copy content of template to it
new_filename="${current_date}_${formatted_title}.md"
cp 00000000_template.md "$new_filename"
echo "Created new design document with name $new_filename from template."
