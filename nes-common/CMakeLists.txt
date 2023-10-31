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

# Generate protobuf and grpc files and add them to individual nes-grpc target
set(PROTO_PATH "${PROJECT_SOURCE_DIR}/grpc")
set(PROTO_FILE_PATH "${PROTO_PATH}/*.proto")

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/nes-grpc/)
file(GLOB PROTO_FILES "${PROTO_FILE_PATH}")
foreach (PROTO_FILE ${PROTO_FILES})
    get_filename_component(FILENAME ${PROTO_FILE} "NAME")
    string(REGEX REPLACE "[.]proto$" ".pb.cc" OUTPUT_SOURCE ${CMAKE_BINARY_DIR}/nes-grpc/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".pb.h" OUTPUT_HEADER ${CMAKE_BINARY_DIR}/nes-grpc/${FILENAME})
    list(APPEND OUTPUT_HEADER ${OUTPUT_HEADER})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.cc" OUTPUT_SOURCE ${CMAKE_BINARY_DIR}/nes-grpc/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.h" OUTPUT_HEADER ${CMAKE_BINARY_DIR}/nes-grpc/${FILENAME})
    list(APPEND OUTPUT_HEADER ${OUTPUT_HEADER})
endforeach ()

add_custom_command(
        OUTPUT ${OUTPUT_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_BINARY_DIR}/nes-grpc/"
        --cpp_out "${CMAKE_BINARY_DIR}/nes-grpc/"
        -I "${PROTO_PATH}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PROTO_FILE_PATH}"
        DEPENDS "${PROTO_FILES}")

macro(target_public_headers TARGET)
    set_target_properties(${TARGET} PROPERTIES PUBLIC_HEADER "${ARGN}")
endmacro()

# Add nes-grpc library
add_library(nes-grpc SHARED ${OUTPUT_SOURCES})
target_link_libraries(nes-grpc PRIVATE ${GRPC_LIBRARIES})
target_include_directories(nes-grpc PUBLIC
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/nes-grpc>
        $<INSTALL_INTERFACE:include/nebulastream/>)

install(TARGETS nes-grpc
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)


get_source(nes-common NES_COMMON_SOURCE_FILES)

# Add Library
add_library(nes-common SHARED ${NES_COMMON_SOURCE_FILES})
target_link_libraries(nes-common PUBLIC nes-grpc ${LIBRARIES})
target_include_directories(nes-common PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_property(TARGET nes-common PROPERTY INTERFACE_LINK_LIBRARIES
        $<BUILD_INTERFACE:${LIBRARIES}>
        $<INSTALL_INTERFACE:>)


register_public_header_dir(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/ Util)
register_public_header_dir(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Exceptions/ Exceptions)
register_public_header_file(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Identifiers.hpp Identifiers.hpp)
register_public_header_file(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/FaultToleranceType.hpp Util/FaultToleranceType.hpp)
register_public_header_file(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/MetricCollectorType.hpp Util/MetricCollectorType.hpp)
register_public_header_file(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/LineageType.hpp Util/LineageType.hpp)
register_public_header_file(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Nodes/Node.hpp Nodes/Node.hpp)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-common PRIVATE "include/PrecompiledHeaders.hpp")
endif ()

# Unikernel Support
get_source(nes-unikernel-support-common NES_UNIKERNEL_SUPPORT_COMMON_SOURCE_FILES)
add_library(nes-unikernel-support-common STATIC ${NES_UNIKERNEL_COMMON_SOURCE_FILES} ${NES_UNIKERNEL_SUPPORT_COMMON_SOURCE_FILES})
target_link_libraries(nes-unikernel-support-common PUBLIC spdlog::spdlog zmq-fork Folly::folly)
target_include_directories(nes-unikernel-support-common PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_property(TARGET nes-unikernel-support-common PROPERTY INTERFACE_LINK_LIBRARIES
        $<BUILD_INTERFACE: spdlog::spdlog zmq-fork Folly::folly>
        $<INSTALL_INTERFACE:>)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-common
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)