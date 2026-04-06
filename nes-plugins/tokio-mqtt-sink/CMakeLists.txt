# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Tokio MQTT sink plugin.
# Registers with TokioSink and SinkValidation registries (both in nes-sinks).
# The Rust sink implementation is in nes_tokio_mqtt_sink crate, linked via umbrella.
# The C++ registration file creates TokioSink with makeMqttSinkSpawnFn().
add_plugin(TokioMqttSink TokioSink nes-sinks TokioMqttSink.cpp)
add_plugin(TokioMqttSink SinkValidation nes-sinks TokioMqttSink.cpp)

# Bats integration test: source→query→sink round-trip via MQTT broker.
if (ENABLE_BATS_TESTS AND NES_ENABLES_TESTS)
    add_test(
            NAME mqtt-sink-embedded-test
            COMMAND
            env NES_REPL=$<TARGET_FILE:nes-repl-embedded>
            env NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
            ${BATS} --verbose-run --timing ${CMAKE_CURRENT_SOURCE_DIR}/tests/mqtt_sink.bats
    )
    set_tests_properties(mqtt-sink-embedded-test PROPERTIES LABELS "Systest;MQTT")
endif ()
