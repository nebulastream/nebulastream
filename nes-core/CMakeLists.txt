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

message(STATUS "Found the following Proto files: " ${OUTPUT_SOURCES})
message(STATUS "Found the following Proto header: " ${OUTPUT_HEADER})

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

add_library(nes-grpc STATIC ${OUTPUT_SOURCES})
target_link_libraries(nes-grpc PUBLIC ${GRPC_LIBRARIES})
target_include_directories(nes-grpc PUBLIC $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/nes-grpc>)

# Add Library
get_source(nes-core NES_CORE_SOURCE_FILES)
message(STATUS "Found the following NES_CORE_SOURCE_FILES files: " ${NES_CORE_SOURCE_FILES})
add_library(nes SHARED ${NES_CORE_SOURCE_FILES} ${OUTPUT_SOURCES})
target_include_directories(nes PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
#target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(nes PRIVATE ${LIBRARIES} ${PROTOBUF_LIBRARIES} )
target_link_libraries(nes PUBLIC nes-common nes-compiler)
target_link_libraries(nes PRIVATE nes-grpc)

get_target_property(LIBA_INCLUDES nes INCLUDE_DIRECTORIES)
foreach(dir ${LIBA_INCLUDES})
    string(APPEND INCLUDE_COMPILER_STRING "/I${dir} ")
    message(STATUS "LIBNES inclues ${dir}")
endforeach()

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")

#TODO select the headers to copy more fine-grained
register_public_header(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/)

if (GRPC_LIBRARIES)
    target_link_libraries(nes PRIVATE nes-grpc)
endif ()

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PUBLIC nes)
target_include_directories(nesCoordinator PUBLIC "include")
target_include_directories(nesCoordinator PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(nesCoordinator PRIVATE nes-grpc)


add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC nes )
target_include_directories(nesWorker PUBLIC "include")
target_include_directories(nesWorker PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(nesWorker  PRIVATE nes-grpc)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()



#Add the nes lib and headers to cpack
install(TARGETS nes
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)
message(STATUS "nes lib dir ${CMAKE_INSTALL_FULL_LIBDIR}")

install(TARGETS nesCoordinator nesWorker
        RUNTIME
        COMPONENT nesExecutables)

include(GNUInstallDirs)
install(DIRECTORY ${CMAKE_BINARY_DIR}/include/nebulastream
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY ${CLANG_INCLUDE_DIR}
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_BINDIR}/lib/clang/${LLVM_PACKAGE_VERSION}/include/)