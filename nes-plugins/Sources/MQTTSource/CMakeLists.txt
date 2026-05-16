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
# broker. The publisher subscribes to the data topic's `_nes_control` sibling,
# waits for the source to publish "ready", then streams rows on the data topic
# and writes a zero-byte sentinel to mark EOS. systest dispatches into these
# when the .test file has `ATTACH INLINE` / `ATTACH FILE` under an MQTT
# physical source — see RegisterMQTT{Inline,File}Data in MQTTSource.cpp.
add_plugin_as_library(MQTT InlineData nes-sources-registry mqtt_inline_data_plugin_library MQTTSource.cpp)
add_plugin_as_library(MQTT FileData nes-sources-registry mqtt_file_data_plugin_library MQTTSource.cpp)

find_package(PahoMqttCpp CONFIG REQUIRED)
# vcpkg builds Paho C++ as a static library (PahoMqttCpp::paho-mqttpp3-static);
# distro/nixpkgs builds typically ship only the shared target (PahoMqttCpp::paho-mqttpp3).
if (TARGET PahoMqttCpp::paho-mqttpp3-static)
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3-static)
else ()
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3)
endif ()

foreach (_lib mqtt_source_plugin_library mqtt_source_validation_plugin_library mqtt_inline_data_plugin_library mqtt_file_data_plugin_library)
    target_include_directories(${_lib} PUBLIC include PRIVATE .)
    target_link_libraries(${_lib} PRIVATE ${PAHO_MQTTPP3_TARGET})
endforeach ()

# External-dependency systests: every `*.test` file under `systests/` that
# declares `# requires: mqtt` is registered as a ctest entry by the macro.
# The co-located `systest-profile/` brings up the broker; the in-binary
# dispatcher (ExternalSystestDispatch.cpp) drives docker compose and the test
# runs in-process. add_external_systest_profile is a no-op when
# ENABLE_DOCKER_TESTS is OFF, so this needs no if() guard.
add_external_systest_profile(
        PROFILE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/systest-profile
)
