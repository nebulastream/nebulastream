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

if (NES_USE_PCH)
    if (NES_ONLY_REQUIRED_HEADERS)
        message( FATAL_ERROR "You can't run NES_USE_PCH with NES_ONLY_REQUIRED_HEADERS. CMake will exit...")
    endif()
    set (BENCHMARK_MINIMAL 0)
    set (BENCHMARK_WINDOW 0)
    if (NES_BENCHMARK_MINIMAL)
        add_compile_definitions(NES_BENCHMARK_MINIMAL)
        set (BENCHMARK_MINIMAL 1)
    endif()
    if (NES_BENCHMARK_WINDOW)
        add_compile_definitions(NES_BENCHMARK_WINDOW)
        set (BENCHMARK_WINDOW 1)
    endif()
        message("NES_BENCHMARK_MINIMAL: ${BENCHMARK_MINIMAL}")
    message("Using precompiled headers for query compilation")
    #set(NES_PCH_FLAGS "-ftime-trace -fpic -Wparentheses-equality -fno-trigraphs -DNES_LOGGING_DEBUG_LEVEL=1 -std=c++${CMAKE_CXX_STANDARD}")
    add_compile_definitions(NES_USE_PCH)
    add_custom_target(
            build_pch_o0 ALL
            COMMAND ${CMAKE_CXX_COMPILER}
            -I${PATH_TO_NES_SOURCE_CODE}/nes-core/include/
            -I${PATH_TO_DEB_SOURCE_CODE}/include/ -ftime-trace -fpic -Wparentheses-equality -fno-trigraphs -DNES_LOGGING_DEBUG_LEVEL=1 -std=c++20
            -march=native -mtune=native -DNES_BENCHMARK_MINIMAL=${BENCHMARK_MINIMAL} -DNES_BENCHMARK_WINDOW=${BENCHMARK_WINDOW}
            -O0 -x c++-header
            ${PATH_TO_NES_SOURCE_CODE}/nes-core/include/QueryCompiler/Compiler/RuntimeHeaders.hpp -o RuntimeHeaders_O0.h.pch
    )
    add_custom_target(
            build_pch_o3 ALL
            COMMAND ${CMAKE_CXX_COMPILER}
            -I${PATH_TO_NES_SOURCE_CODE}/nes-core/include/
            -I${PATH_TO_DEB_SOURCE_CODE}/include/ -ftime-trace -fpic -Wparentheses-equality -fno-trigraphs -DNES_LOGGING_DEBUG_LEVEL=1 -std=c++20
            -march=native -mtune=native -DNES_BENCHMARK_MINIMAL=${BENCHMARK_MINIMAL} -DNES_BENCHMARK_WINDOW=${BENCHMARK_WINDOW}
             -O3 -x c++-header
            ${PATH_TO_NES_SOURCE_CODE}/nes-core/include/QueryCompiler/Compiler/RuntimeHeaders.hpp -o RuntimeHeaders_O3.h.pch
    )
    add_dependencies(nes build_pch_o0)
    add_dependencies(nes build_pch_o3)
endif()

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
        DESTINATION "/nebulastream")