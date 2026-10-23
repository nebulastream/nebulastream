# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generally, when building a plugin, you can internally structure and link it how you want, you only need to connect
# it correctly to the registry entry glue code, as is shown below

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# One implementation library serving all registries this plugin participates in.
add_library(mqtt_source_plugin_library STATIC MQTTSource.cpp)
target_link_libraries(mqtt_source_plugin_library PRIVATE nes-sources)
target_include_directories(mqtt_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

find_package(PahoMqttCpp CONFIG REQUIRED)
target_link_libraries(mqtt_source_plugin_library PRIVATE PahoMqttCpp::paho-mqttpp3-static)

# This call ensures that the glue code that loads a plugin, which is part of the registry target, can refer to some symbols from the MQTT source header,
# which get resolved during the final link time when everything is assembled into one big binary (interface link).
# Als, it creates a direct dependency of this plugin on its library (private link).
link_plugin_library(nes-sources mqtt_source_plugin_library)
# These calls generate some glue-code for loading the plugin that is added to the registries target
# It needs to include the headers of this MQTT Source so that it can refer to it.
# That means that the header declaring the symbols (in this case the source constructor and the validateAndFormat function)
# Needs to follow the naming convention set out by the registry, so that it can be found automatically.
# For sources its "\${PLUGIN_NAME}Source.hpp" , so the header for the plugin named MQTT must be MQTTSource.hpp
add_registry_entry(Source MQTT)
add_registry_entry(SourceValidation MQTT)
# These tests use docker CLI to run docker-compose from within the development container.
add_e2e_test(
    NAME mqtt-source-test
    BATS_FILE tests/distributed.bats
    DOCKER_COMPOSE
    JOBS 4
)
