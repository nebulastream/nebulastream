# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(RdKafka CONFIG REQUIRED)

add_nes_test(kafka-source-config-test KafkaSourceConfigTest.cpp)
target_link_libraries(kafka-source-config-test kafka_source_plugin_library nes-sources RdKafka::rdkafka++)

if (SANITIZER_OPTION STREQUAL "thread")
    # librdkafka's built-in mock cluster, which the source tests use for in-process testing, segfaults at
    # construction under ThreadSanitizer inside librdkafka's own vendored source, so the whole target is skipped.
    message(WARNING "kafka-source-unit-test is disabled under the thread sanitizer: librdkafka's mock cluster segfaults at construction.")
else ()
    add_nes_test(kafka-source-unit-test KafkaSourceTest.cpp)
    target_link_libraries(kafka-source-unit-test kafka_source_plugin_library nes-sources nes-memory RdKafka::rdkafka RdKafka::rdkafka++)
endif ()
