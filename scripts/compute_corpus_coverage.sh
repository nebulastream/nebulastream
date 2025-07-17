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

set -eo pipefail

cd "$(git rev-parse --show-toplevel)"

if [ "$#" -ne 2 ]
then
    cat << EOF
pls give
- build dir   as 1st param
- nes-corpora as 2nd param
EOF
    exit 1
fi

BASE_DIR=$(pwd)
BUILD_DIR=$1
CORPUS=$2

find . -name "*.gcno" -delete
find . -name "*.gcda" -delete



# cmake --build "$BUILD_DIR"    --target clean
# cmake --build "$BUILD_DIR" -j --target snw-text-fuzz snw-strict-fuzz snw-proto-fuzz

TMP_DIR=$(mktemp -d)

echo "working in $TMP_DIR"

cd "$TMP_DIR"


for fuzzer_exe in snw-text-fuzz snw-strict-fuzz snw-proto-fuzz
do
    find "$CORPUS/$fuzzer_exe" -type f -print0 | xargs -0 --max-args=1 --max-procs=$(nproc) "$BASE_DIR/$BUILD_DIR/nes-single-node-worker/fuzz/$fuzzer_exe"
done
