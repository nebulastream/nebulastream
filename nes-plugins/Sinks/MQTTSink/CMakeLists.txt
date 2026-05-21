# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(MQTT Sink nes-sinks-registry mqtt_sink_plugin_library MQTTSink.cpp)
add_plugin_as_library(MQTT SinkValidation nes-sinks-registry mqtt_sink_validation_plugin_library MQTTSink.cpp)

# The SinkCapture plugin lives in MQTTTestSubscriber.cpp so MQTTSink.{hpp,cpp}
# stays a pure runtime publisher with no test-scaffold awareness. The test
# subscriber rewrites the sink's broker endpoint to the dispatcher-allocated
# host port and drains the broker into the systest result file — the sink-side
# mirror of MQTTSource's MQTTTestPublisher.
add_plugin_as_library(MQTT SinkCapture nes-sinks-registry mqtt_sink_capture_plugin_library MQTTTestSubscriber.cpp)

find_package(PahoMqttCpp CONFIG REQUIRED)
# vcpkg builds Paho C++ as a static library (PahoMqttCpp::paho-mqttpp3-static);
# distro/nixpkgs builds typically ship only the shared target (PahoMqttCpp::paho-mqttpp3).
if (TARGET PahoMqttCpp::paho-mqttpp3-static)
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3-static)
else ()
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3)
endif ()

foreach (_lib mqtt_sink_plugin_library mqtt_sink_validation_plugin_library mqtt_sink_capture_plugin_library)
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
