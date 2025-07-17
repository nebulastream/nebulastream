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

if [ -v NES_PREBUILT_VCPKG_ROOT ]
then
    echo running inside docker
    BASE_DIR=/nes
    BUILD_DIR=build

    if [ -z "$FWC_NES_CORPORA_TOKEN" ]
    then
        echo pls set FWC_NES_CORPORA_TOKEN
        exit 1
    fi

    git clone "https://fwc:$FWC_NES_CORPORA_TOKEN@github.com/fwc/nes-corpora.git" /nes-corpora
    CORPUS=/nes-corpora
    exit 0
elif [ "$#" -eq 2 ]
then
    BASE_DIR=$(pwd)
    BUILD_DIR=$1
    CORPUS=$2
else
    cat << EOF
pls give
- build dir   as 1st param
- nes-corpora as 2nd param

or run in docker
EOF
    exit 1
fi


find . -name "*.gcno" -delete
find . -name "*.gcda" -delete

TMP_DIR=$(mktemp -d)

echo "working in $TMP_DIR"

cd "$TMP_DIR"


for fuzzer_exe in snw-text-fuzz snw-strict-fuzz snw-proto-fuzz
do
    echo checking corpus for $fuzzer_exe
    find "$CORPUS/$fuzzer_exe" -type f -print0 | xargs -0 --max-args=1 --max-procs="$(nproc)" timeout 6m "$BASE_DIR/$BUILD_DIR/nes-single-node-worker/fuzz/$fuzzer_exe" -timeout=300 > $fuzzer_exe.log 2> $fuzzer_exe.err || true
done
