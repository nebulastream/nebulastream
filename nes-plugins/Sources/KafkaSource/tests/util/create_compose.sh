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

# The Kafka source reuses the Kafka sink's compose setup (single-node KRaft broker, kafka-client, nes-cli, workers).
# The network label comes from NES_BATS_TEST_LABEL, which the bats suite sets to its own name.

set -eo pipefail

exec "${NES_DIR:?NES_DIR is set by add_e2e_test}/nes-plugins/Sinks/KafkaSink/tests/util/create_compose.sh" "$@"
