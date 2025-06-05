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
