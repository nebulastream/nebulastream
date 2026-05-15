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
# InlineData / FileData plugins materialise the test's ATTACH content into the
# broker (publish each row to a unique retained topic). systest dispatches into
# these when the .test file has `ATTACH INLINE` / `ATTACH FILE` under an MQTT
# physical source — see RegisterMQTT{Inline,File}Data in MQTTSource.cpp.
add_plugin_as_library(MQTT InlineData nes-sources-registry mqtt_inline_data_plugin_library MQTTSource.cpp)
add_plugin_as_library(MQTT FileData nes-sources-registry mqtt_file_data_plugin_library MQTTSource.cpp)

foreach (_lib mqtt_source_plugin_library mqtt_source_validation_plugin_library mqtt_inline_data_plugin_library mqtt_file_data_plugin_library)
    target_include_directories(${_lib} PUBLIC include PRIVATE .)
endforeach ()

find_package(PahoMqttCpp CONFIG REQUIRED)
# vcpkg builds Paho C++ as a static library (PahoMqttCpp::paho-mqttpp3-static);
# distro/nixpkgs builds typically ship only the shared target (PahoMqttCpp::paho-mqttpp3).
if (TARGET PahoMqttCpp::paho-mqttpp3-static)
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3-static)
else ()
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3)
endif ()
foreach (_lib mqtt_source_plugin_library mqtt_source_validation_plugin_library mqtt_inline_data_plugin_library mqtt_file_data_plugin_library)
    target_link_libraries(${_lib} PRIVATE ${PAHO_MQTTPP3_TARGET})
endforeach ()

# External-dependency systest: a `# requires: mqtt` test file plus the
# co-located docker-compose snippet that brings up the broker and pre-seeds
# retained messages. add_external_systest_profile is a no-op when
# ENABLE_BATS_TESTS / ENABLE_DOCKER_TESTS are OFF, so this needs no guard.
add_external_systest_profile(
        NAME mqtt
        TEST_FILE ${CMAKE_CURRENT_SOURCE_DIR}/systests/MQTT.test
        PROFILE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/systest-profile
)
add_external_systest_profile(
        NAME mqtt
        CTEST_NAME mqtt-ysb-systest-external
        TEST_FILE ${CMAKE_CURRENT_SOURCE_DIR}/systests/MQTT_YSB.test
        PROFILE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/systest-profile
)
