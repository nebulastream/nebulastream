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

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/grpc_generated_src/)
file(GLOB PROTO_FILES "${PROTO_FILE_PATH}")
foreach (PROTO_FILE ${PROTO_FILES})
    get_filename_component(FILENAME ${PROTO_FILE} "NAME")
    string(REGEX REPLACE "[.]proto$" ".pb.cc" OUTPUT_SOURCE ${CMAKE_BINARY_DIR}/grpc_generated_src/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".pb.h" OUTPUT_HEADER ${CMAKE_BINARY_DIR}/grpc_generated_src/${FILENAME})
    list(APPEND OUTPUT_HEADER ${OUTPUT_HEADER})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.cc" OUTPUT_SOURCE ${CMAKE_BINARY_DIR}/grpc_generated_src/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.h" OUTPUT_HEADER ${CMAKE_BINARY_DIR}/grpc_generated_src/${FILENAME})
    list(APPEND OUTPUT_HEADER ${OUTPUT_HEADER})
endforeach ()

add_custom_command(
        OUTPUT ${OUTPUT_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "generate_mock_code=true:${CMAKE_BINARY_DIR}/grpc_generated_src/"
        --cpp_out "${CMAKE_BINARY_DIR}/grpc_generated_src/"
        -I "${PROTO_PATH}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PROTO_FILE_PATH}"
        DEPENDS "${PROTO_FILES}")

macro(target_public_headers TARGET)
    set_target_properties(${TARGET} PROPERTIES PUBLIC_HEADER "${ARGN}")
endmacro()

# Add nes-grpc library
add_library(nes-grpc ${OUTPUT_SOURCES})
target_link_libraries(nes-grpc PUBLIC ${GRPC_LIBRARIES})

target_include_directories(nes-grpc PUBLIC
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/grpc_generated_src>
        $<INSTALL_INTERFACE:include/nebulastream/>)

get_source(nes-common NES_COMMON_SOURCE_FILES)

# Add Library
add_library(nes-common ${NES_COMMON_SOURCE_FILES})

find_package(cpptrace   CONFIG REQUIRED)
find_package(fmt        CONFIG REQUIRED)
find_package(libcuckoo  CONFIG REQUIRED)
find_package(magic_enum CONFIG REQUIRED)
find_package(spdlog     CONFIG REQUIRED)
find_package(yaml-cpp   CONFIG REQUIRED)
find_package(folly      CONFIG REQUIRED)
target_link_libraries(nes-common PUBLIC nes-grpc cpptrace::cpptrace fmt::fmt libcuckoo::libcuckoo magic_enum::magic_enum spdlog::spdlog yaml-cpp::yaml-cpp folly::folly rustlogger_cxx_bridge)

target_include_directories(nes-common PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-common PRIVATE "include/PrecompiledHeaders.hpp")
endif ()

add_tests_if_enabled(tests)
