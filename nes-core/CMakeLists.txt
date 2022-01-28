# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Add Source Code
add_subdirectory(src)

# Proto file
set(PROTO_PATH "${PROJECT_SOURCE_DIR}/grpc")
set(PROTO_FILE_PATH "${PROTO_PATH}/*.proto")

file(GLOB PROTO_FILES "${PROTO_FILE_PATH}")
foreach (PROTO_FILE ${PROTO_FILES})
    get_filename_component(FILENAME ${PROTO_FILE} "NAME")
    string(REGEX REPLACE "[.]proto$" ".pb.cc" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".pb.h" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.cc" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.h" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
endforeach ()

message(STATUS "Found the following Proto files: " ${OUTPUT_SOURCES})

add_custom_command(
        OUTPUT ${OUTPUT_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${PROTO_PATH}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PROTO_FILE_PATH}"
        DEPENDS "${PROTO_FILE_PATH}")

# Add Library
get_source_nes(nes_SOURCE_FILES)
get_header_nes(nes_HEADER_FILES)
add_library(nes SHARED ${nes_SOURCE_FILES} ${nes_HEADER_FILES} ${OUTPUT_SOURCES} ${OUTPUT_HEADERS})
target_include_directories(nes PUBLIC "include")
target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/../")
target_link_libraries(nes PUBLIC ${LIBRARIES} ${PROTOBUF_LIBRARIES})

if (GRPC_LIBRARIES)
    target_link_libraries(nes PRIVATE ${GRPC_LIBRARIES})
endif ()

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PRIVATE ${LIBRARIES} nes)
target_include_directories(nesCoordinator PUBLIC "include")
target_include_directories(nesCoordinator PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesCoordinator PRIVATE ${GRPC_LIBRARIES})
endif ()

add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC ${LIBRARIES} nes)
target_include_directories(nesWorker PUBLIC "include")
target_include_directories(nesWorker PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesWorker PRIVATE ${GRPC_LIBRARIES})
endif ()
if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()


#Add the nes lib and headers to cpack
install(TARGETS nes
        COMPONENT nesLibrary
        DESTINATION lib)

install(TARGETS nesCoordinator nesWorker
        COMPONENT nesExecutables
        RUNTIME
        DESTINATION "/opt/local/nebula-stream")

install(DIRECTORY "include"
        COMPONENT nesHeaders
        DESTINATION "/nebulastream/nes-core")