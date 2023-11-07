# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-connectors NES_CONNECTORS_SOURCE_FILES)

# Add Library
add_library(nes-connectors SHARED ${NES_CONNECTORS_SOURCE_FILES})
target_link_libraries(nes-connectors PRIVATE nes-common nes-configurations nes-data-types)
target_include_directories(nes-connectors PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Library containing Kafka client
if (NES_USE_KAFKA)
    # TODO #3351 The commands should be moved into a separate CMake config file.
    if (NOT APPLE)
        find_package(cppkafka REQUIRED)
        target_link_libraries(nes-connectors PRIVATE cppkafka::cppkafka )
    else ()
        message(WARNING "ENABLE_KAFKA_BUILD Not supported on OSX")
    endif ()
endif ()

# Library containing MQTT client
if (NES_USE_MQTT)
    message("VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
    find_package(PahoMqttCpp CONFIG REQUIRED)
    target_link_libraries(nes-connectors PRIVATE PahoMqttCpp::paho-mqttpp3-static)
endif (NES_USE_MQTT)

# Add Private Dependencies
target_link_libraries(nes-connectors PRIVATE ${LIBRARIES_ZMQ})

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-connectors
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)