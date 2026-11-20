# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Client of the Calcite planner service (nes-calcite-planner/, Java) and the Substrait -> LogicalPlan conversion.
# Substrait protos are vendored from io.substrait:protobuf 0.106.0, the version the Java side uses.

set(CALCITE_SUBSTRAIT_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(CALCITE_SERVICE_PROTO_DIR ${PROJECT_SOURCE_DIR}/nes-calcite-planner/src/main/proto)
set(CALCITE_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${CALCITE_GENERATED_DIR})

set(CALCITE_SUBSTRAIT_PROTOS
        substrait/algebra.proto
        substrait/extended_expression.proto
        substrait/plan.proto
        substrait/type.proto
        substrait/extensions/extensions.proto)
set(CALCITE_GENERATED_SOURCES)
foreach (PROTO ${CALCITE_SUBSTRAIT_PROTOS})
    string(REGEX REPLACE "[.]proto$" ".pb.cc" GENERATED ${PROTO})
    list(APPEND CALCITE_GENERATED_SOURCES ${CALCITE_GENERATED_DIR}/${GENERATED})
endforeach ()
list(TRANSFORM CALCITE_SUBSTRAIT_PROTOS PREPEND ${CALCITE_SUBSTRAIT_PROTO_DIR}/ OUTPUT_VARIABLE CALCITE_SUBSTRAIT_PROTO_FILES)

get_target_property(CALCITE_PROTOBUF_INCLUDE protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
list(GET CALCITE_PROTOBUF_INCLUDE 0 CALCITE_PROTOBUF_INCLUDE)

add_custom_command(
        OUTPUT ${CALCITE_GENERATED_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --cpp_out ${CALCITE_GENERATED_DIR} -I ${CALCITE_SUBSTRAIT_PROTO_DIR} -I ${CALCITE_PROTOBUF_INCLUDE} ${CALCITE_SUBSTRAIT_PROTO_FILES}
        DEPENDS ${CALCITE_SUBSTRAIT_PROTO_FILES})

set(CALCITE_SERVICE_SOURCES
        ${CALCITE_GENERATED_DIR}/nes_calcite_planner.pb.cc
        ${CALCITE_GENERATED_DIR}/nes_calcite_planner.grpc.pb.cc)
add_custom_command(
        OUTPUT ${CALCITE_SERVICE_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --cpp_out ${CALCITE_GENERATED_DIR} --grpc_out ${CALCITE_GENERATED_DIR}
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        -I ${CALCITE_SERVICE_PROTO_DIR} -I ${CALCITE_SUBSTRAIT_PROTO_DIR} -I ${CALCITE_PROTOBUF_INCLUDE}
        ${CALCITE_SERVICE_PROTO_DIR}/nes_calcite_planner.proto
        DEPENDS ${CALCITE_SERVICE_PROTO_DIR}/nes_calcite_planner.proto ${CALCITE_GENERATED_SOURCES})

add_custom_target(nes-calcite-codegen DEPENDS ${CALCITE_GENERATED_SOURCES} ${CALCITE_SERVICE_SOURCES})
add_dependencies(nes-codegen nes-calcite-codegen)

# The pipelines of a worker's part of a placed physical plan: the coordinator resolves the part, the worker builds its
# pipelines from it. Workers link only this, not the planner client.
add_library(nes-calcite-pipelines STATIC
        src/SubstraitToPhysicalPlan.cpp
        src/SubstraitTypes.cpp
        ${CALCITE_GENERATED_SOURCES}
        ${CALCITE_GENERATED_DIR}/nes_calcite_planner.pb.cc)
add_dependencies(nes-calcite-pipelines nes-calcite-codegen)
target_include_directories(nes-calcite-pipelines
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<BUILD_INTERFACE:${CALCITE_GENERATED_DIR}>)
target_link_libraries(nes-calcite-pipelines PUBLIC nes-inference nes-distributed nes-physical-operators nes-query-compiler nes-input-formatters nes-sinks nes-sources nes-common ${GRPC_LIBRARIES})

add_library(nes-calcite STATIC
        src/CalcitePlanner.cpp
        src/QueryOptimizerConfiguration.cpp
        ${CALCITE_GENERATED_DIR}/nes_calcite_planner.grpc.pb.cc)
target_include_directories(nes-calcite
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<BUILD_INTERFACE:${CALCITE_GENERATED_DIR}>)
target_link_libraries(nes-calcite PUBLIC nes-calcite-pipelines nes-configurations nes-network-interface nes-physical-operators nes-query-compiler nes-input-formatters nes-sinks nes-sql-parser nes-sources nes-common ${GRPC_LIBRARIES})
# Generated protobuf code does not follow our warning rules.
set_source_files_properties(${CALCITE_GENERATED_SOURCES} ${CALCITE_SERVICE_SOURCES} PROPERTIES COMPILE_OPTIONS "-w")

# With GraalVM, the planner is built into a native library (native-image --shared) and called in-process: no planner
# service. Without it, the planner runs as a service (see nes-calcite-planner/README.md). GraalVM comes from
# NES_CALCITE_GRAALVM_HOME or else $GRAALVM_HOME (the development image sets it). Rebuilt only when the planner's sources
# change; native-image itself always does a full build (about 1.5 min, 9 GB RAM).
set(NES_CALCITE_GRAALVM_HOME "" CACHE PATH "GraalVM; if set, the Calcite planner is linked in as a native library")
if (NOT NES_CALCITE_GRAALVM_HOME AND DEFINED ENV{GRAALVM_HOME})
    set(NES_CALCITE_GRAALVM_HOME "$ENV{GRAALVM_HOME}")
endif ()
if (NES_CALCITE_GRAALVM_HOME)
    find_program(NES_CALCITE_MAVEN mvn DOC "Maven, to build the planner jar for the native library")
    if (NOT NES_CALCITE_MAVEN)
        message(FATAL_ERROR "Maven (mvn) is required to build the Calcite planner for the native library")
    endif ()
    message(STATUS "Calcite planner: native library (GraalVM ${NES_CALCITE_GRAALVM_HOME})")
    set(CALCITE_PLANNER_DIR ${PROJECT_SOURCE_DIR}/nes-calcite-planner)
    set(CALCITE_PLANNER_JAR ${CALCITE_PLANNER_DIR}/target/nes-calcite-planner.jar)
    set(CALCITE_NATIVE_DIR ${CMAKE_CURRENT_BINARY_DIR}/native)
    set(CALCITE_NATIVE_LIBRARY ${CALCITE_NATIVE_DIR}/libnes-calcite-planner.so)
    file(GLOB_RECURSE CALCITE_PLANNER_SOURCES CONFIGURE_DEPENDS ${CALCITE_PLANNER_DIR}/src/main/*)
    add_custom_command(
            OUTPUT ${CALCITE_PLANNER_JAR}
            COMMAND ${CMAKE_COMMAND} -E env JAVA_HOME=${NES_CALCITE_GRAALVM_HOME}
            ${NES_CALCITE_MAVEN} -q -f ${CALCITE_PLANNER_DIR}/pom.xml package -DskipTests
            DEPENDS ${CALCITE_PLANNER_SOURCES} ${CALCITE_PLANNER_DIR}/pom.xml
            COMMENT "Building the Calcite planner jar")
    add_custom_command(
            OUTPUT ${CALCITE_NATIVE_LIBRARY} ${CALCITE_NATIVE_DIR}/libnes-calcite-planner.h
            COMMAND ${CMAKE_COMMAND} -E make_directory ${CALCITE_NATIVE_DIR}
            COMMAND ${NES_CALCITE_GRAALVM_HOME}/bin/native-image --shared --silent -cp ${CALCITE_PLANNER_JAR}
            -o ${CALCITE_NATIVE_DIR}/libnes-calcite-planner
            DEPENDS ${CALCITE_PLANNER_JAR}
            COMMENT "Compiling the Calcite planner into a native library (GraalVM native-image)")
    add_custom_target(nes-calcite-planner-native DEPENDS ${CALCITE_NATIVE_LIBRARY})
    add_library(nes-calcite-planner-native-library SHARED IMPORTED)
    set_target_properties(nes-calcite-planner-native-library PROPERTIES
            IMPORTED_LOCATION ${CALCITE_NATIVE_LIBRARY}
            IMPORTED_NO_SONAME TRUE)
    add_dependencies(nes-calcite nes-calcite-planner-native)
    target_include_directories(nes-calcite PRIVATE ${CALCITE_NATIVE_DIR})
    target_link_libraries(nes-calcite PUBLIC nes-calcite-planner-native-library)
    target_compile_definitions(nes-calcite PRIVATE NES_CALCITE_NATIVE)
    set(NES_CALCITE_NATIVE_LIBRARY ${CALCITE_NATIVE_LIBRARY} CACHE INTERNAL "The native Calcite planner library")
else ()
    message(STATUS "Calcite planner: service (no GraalVM)")
    unset(NES_CALCITE_NATIVE_LIBRARY CACHE)
endif ()
