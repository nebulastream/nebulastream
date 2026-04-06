# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Zero C++ — Rust-only Tokio MQTT source plugin.
# The source is built as part of the generated nes_source_registry crate
# and linked at link time via the umbrella.
add_rust_tokio_source(TokioMqtt nes_tokio_mqtt_source::create_mqtt_source "${CMAKE_CURRENT_SOURCE_DIR}")

# Bats integration test: starts embedded REPL with TokioMqtt source,
# publishes messages via mosquitto_pub, and verifies sink output.
if (ENABLE_BATS_TESTS AND NES_ENABLES_TESTS)
    add_test(
            NAME mqtt-source-embedded-test
            COMMAND
            env NES_REPL=$<TARGET_FILE:nes-repl-embedded>
            env NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
            ${BATS} --verbose-run --timing ${CMAKE_CURRENT_SOURCE_DIR}/tests/mqtt_source.bats
    )
    set_tests_properties(mqtt-source-embedded-test PROPERTIES LABELS "Systest;MQTT")
endif ()
