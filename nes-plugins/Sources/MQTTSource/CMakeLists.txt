# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# One implementation library serving all registries this plugin participates in.
add_library(mqtt_source_plugin_library STATIC MQTTSource.cpp)
target_link_libraries(mqtt_source_plugin_library PRIVATE nes-sources)
target_include_directories(mqtt_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

find_package(PahoMqttCpp CONFIG REQUIRED)
target_link_libraries(mqtt_source_plugin_library PRIVATE PahoMqttCpp::paho-mqttpp3-static)

link_plugin_library(nes-sources mqtt_source_plugin_library)
add_registry_entry(Source MQTT)
add_registry_entry(SourceConfigSchema MQTT)
add_registry_entry(SourceConfig MQTT)
# These tests use docker CLI to run docker-compose from within the development container.
add_e2e_test(
    NAME mqtt-source-test
    BATS_FILE tests/distributed.bats
    DOCKER_COMPOSE
    JOBS 4
)
