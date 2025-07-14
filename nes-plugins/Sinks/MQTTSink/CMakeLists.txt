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

target_include_directories(mqtt_sink_plugin_library
        PUBLIC include
        PRIVATE .
)
target_include_directories(mqtt_sink_validation_plugin_library
        PUBLIC include
        PRIVATE .
)

find_package(PahoMqttCpp CONFIG REQUIRED)
target_link_libraries(mqtt_sink_plugin_library PRIVATE PahoMqttCpp::paho-mqttpp3-static)
target_link_libraries(mqtt_sink_validation_plugin_library PRIVATE PahoMqttCpp::paho-mqttpp3-static)
