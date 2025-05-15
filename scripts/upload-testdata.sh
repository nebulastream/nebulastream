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

set -e

FILE=$1
FILENAME=$(basename $FILE)
CHECKSUM=$(md5sum $FILE | awk '{print $1}')

cp $FILE /tmp/MD5_$CHECKSUM
gh release upload --repo nebulastream/nes-datasets v1 /tmp/MD5_$CHECKSUM

echo "$CHECKSUM" > $FILENAME.md5
git add $FILENAME.md5
rm $FILE
