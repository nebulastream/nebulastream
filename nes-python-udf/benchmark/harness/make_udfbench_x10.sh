#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Writes each StreamUDFBench tiny file that UdfBenchCodonNumba.test reads ten times into one file, under
# <data>/x10/streamudfbench (default data: /dev/shm/streamudfbench-data), next to <data>/large/streamudfbench/tiny.
set -euo pipefail
data="${1:-/dev/shm/streamudfbench-data}"
mkdir -p "$data/x10/streamudfbench"
for file in artifacts_15M.csv artifact_abstracts_13M.csv artifact_authorlists_4M.csv artifact_charges_32K.csv \
            projects_artifacts_16M.csv query2json_3M.csv; do
    target="$data/x10/streamudfbench/$file"
    : > "$target"
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        cat "$data/large/streamudfbench/tiny/$file" >> "$target"
    done
done
