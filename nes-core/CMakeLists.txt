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
include(GNUInstallDirs)

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

# Add nes-core library
get_source(nes-core NES_CORE_SOURCE_FILES)
add_library(nes SHARED ${NES_CORE_SOURCE_FILES} src/Optimizer/QueryRewrite/DuplicateOperatorEliminationRule.cpp)
target_include_directories(nes PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
target_link_libraries(nes PUBLIC cpr::cpr ${CURL_LIBRARIES} nes-grpc nes-compiler nes-runtime)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes REUSE_FROM nes-common)
endif ()
if (NES_USE_TF)
    message(STATUS "Building with tensorflowlite")
    target_link_libraries(nes PUBLIC tensorflowlite_c::tensorflowlite_c)
endif ()

get_target_property(LIBA_INCLUDES nes INCLUDE_DIRECTORIES)
foreach (dir ${LIBA_INCLUDES})
    string(APPEND INCLUDE_COMPILER_STRING "/I${dir} ")
    message(STATUS "LIBNES includes ${dir}")
endforeach ()

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")

# Select fine-grained headers
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/API/ API)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Windowing Windowing)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Sources Operators/LogicalOperators/Sources)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Sinks Operators/LogicalOperators/Sinks)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/ QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Execution/ Runtime/Execution/)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/State/ State/)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/QueryCompiler/QueryCompilerForwardDeclaration.hpp QueryCompiler/QueryCompilerForwardDeclaration.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/InferModelOperatorHandler.hpp Operators/LogicalOperators/InferModelOperatorHandler.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/RuntimeForwardRefs.hpp Runtime/RuntimeForwardRefs.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Plans/Query/QueryPlan.hpp Plans/Query/QueryPlan.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Nodes/Expressions/FieldAccessExpressionNode.hpp Nodes/Expressions/FieldAccessExpressionNode.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Nodes/Expressions/ExpressionNode.hpp Nodes/Expressions/ExpressionNode.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Nodes/Node.hpp Nodes/Node.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/HashMap.hpp Util/Experimental/HashMap.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Core.hpp Util/Core.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/Hash.hpp Util/Experimental/Hash.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/CRC32Hash.hpp Util/Experimental/CRC32Hash.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/MurMurHash3.hpp Util/Experimental/MurMurHash3.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Exceptions/WindowProcessingException.hpp Exceptions/WindowProcessingException.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/FaultToleranceType.hpp Util/FaultToleranceType.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Monitoring/MetricCollectors/MetricCollectorType.hpp Monitoring/MetricCollectors/MetricCollectorType.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/LineageType.hpp Util/LineageType.hpp)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/folly/ folly)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/spdlog/ spdlog)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/fmt/ fmt)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/glog/ glog)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/boost/ boost)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/gflags/ gflags)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/double-conversion/ double-conversion)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp)

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PUBLIC nes)

add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC nes )

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

install(TARGETS nesCoordinator nesWorker
        RUNTIME
        COMPONENT nesExecutables)

install(DIRECTORY ${CMAKE_BINARY_DIR}/include/nebulastream
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY ${CLANG_INCLUDE_DIR}
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/clang/${LLVM_VERSION_MAJOR}/include/)
