# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(MQTT Source nes-sources-registry mqtt_source_plugin_library MQTTSource.cpp)
add_plugin_as_library(MQTT SourceValidation nes-sources-registry mqtt_source_validation_plugin_library MQTTSource.cpp)

target_include_directories(mqtt_source_plugin_library
        PUBLIC include
        PRIVATE .
)
target_include_directories(mqtt_source_validation_plugin_library
        PUBLIC include
        PRIVATE .
)

find_package(PahoMqttCpp CONFIG REQUIRED)
# vcpkg builds Paho C++ as a static library (PahoMqttCpp::paho-mqttpp3-static);
# distro/nixpkgs builds typically ship only the shared target (PahoMqttCpp::paho-mqttpp3).
if (TARGET PahoMqttCpp::paho-mqttpp3-static)
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3-static)
else ()
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3)
endif ()
target_link_libraries(mqtt_source_plugin_library PRIVATE ${PAHO_MQTTPP3_TARGET})
target_link_libraries(mqtt_source_validation_plugin_library PRIVATE ${PAHO_MQTTPP3_TARGET})

# External-dependency systest: a `# requires: mqtt` test file plus the
# co-located docker-compose snippet that brings up the broker and pre-seeds
# retained messages. add_external_systest_profile is a no-op when
# ENABLE_BATS_TESTS / ENABLE_DOCKER_TESTS are OFF, so this needs no guard.
add_external_systest_profile(
        PROFILE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/systest-profile
)
