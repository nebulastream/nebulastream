
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)

message(STATUS "Fetching Paho MQTT C++ library")

FetchContent_Declare(
    paho_mqtt_cpp
    URL https://github.com/eclipse-paho/paho.mqtt.cpp/archive/refs/tags/v1.3.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_GetProperties(paho_mqtt_cpp)

if (NOT paho_mqtt_cpp_POPULATED)
    message(STATUS "Fetching Paho MQTT C library")

    FetchContent_Declare(
        paho_mqtt_c
        URL https://github.com/eclipse-paho/paho.mqtt.c/archive/refs/tags/v1.3.13.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )

    FetchContent_GetProperties(paho_mqtt_c)

    if (NOT paho_mqtt_c_POPULATED)

        set(PAHO_ENABLE_TESTING OFF CACHE BOOL "")

        FetchContent_MakeAvailable(paho_mqtt_c)

        add_library(paho_mqtt_c::paho-mqtt3a ALIAS paho-mqtt3a)
        message(STATUS "Using asynchronous dynamically linked library")

        set(PAHO_MQTT_C_INCLUDE_DIRS $<TARGET_PROPERTY:paho_mqtt_c::paho-mqtt3a,INCLUDE_DIRECTORIES>)
        set(PAHO_MQTT_C_LIBRARIES ${paho_mqtt_c_BINARY_DIR}/src/libpaho-mqtt3a.so)

        FetchContent_MakeAvailable(paho_mqtt_cpp)

        add_library(paho_mqtt_cpp::paho-mqttpp3 ALIAS paho-mqttpp3)
    endif()
endif()


add_plugin_as_library(MQTT Source nes-sources mqtt_source_plugin_library MQTTSource.cpp)
add_plugin_as_library(MQTT SourceValidation nes-sources mqtt_source_validation_plugin_library MQTTSource.cpp)

target_link_libraries(mqtt_source_plugin_library PRIVATE paho_mqtt_cpp::paho-mqttpp3)
target_link_libraries(mqtt_source_validation_plugin_library PRIVATE paho_mqtt_cpp::paho-mqttpp3)

