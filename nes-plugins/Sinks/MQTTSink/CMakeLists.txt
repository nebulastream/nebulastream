# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
add_plugin_as_library(MQTT Sink nes-sinks-registry mqtt_sink_plugin_library MQTTSink.cpp)
add_plugin_as_library(MQTT SinkValidation nes-sinks-registry mqtt_sink_validation_plugin_library MQTTSink.cpp)

# paho-mqttpp3 (C++ wrapper over paho.mqtt.c) — same dependency the MQTT source uses.
find_package(PahoMqttCpp CONFIG REQUIRED)
if (TARGET PahoMqttCpp::paho-mqttpp3-static)
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3-static)
else ()
    set(PAHO_MQTTPP3_TARGET PahoMqttCpp::paho-mqttpp3)
endif ()
foreach (_lib mqtt_sink_plugin_library mqtt_sink_validation_plugin_library)
    target_include_directories(${_lib} PUBLIC include PRIVATE .)
    target_link_libraries(${_lib} PRIVATE ${PAHO_MQTTPP3_TARGET})
endforeach ()
