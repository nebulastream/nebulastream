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

FILE_NAME=$1
NUMBER_OF_NODES=$(yq '. | length' $FILE_NAME)
NUMBER_OF_NODES=$(($NUMBER_OF_NODES - 1))
cat $FILE_NAME
echo "Starting $NUMBER_OF_NODES nodes"

cargo build
for i in $(seq 0 $NUMBER_OF_NODES); do
  cargo run -- $FILE_NAME $i &
done

wait
