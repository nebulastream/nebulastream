# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# `mqtt-sink-test` is already taken by the bats e2e entry registered in the parent CMakeLists.
add_nes_test(mqtt-sink-unit-test MQTTSinkTest.cpp)
# PahoMqttCpp is linked PRIVATE into mqtt_sink_plugin_library, so its include dir does not
# propagate; MQTTSink.hpp pulls in <mqtt/async_client.h> and needs it here as well.
find_package(PahoMqttCpp CONFIG REQUIRED)
target_link_libraries(mqtt-sink-unit-test mqtt_sink_plugin_library nes-sinks PahoMqttCpp::paho-mqttpp3-static)
