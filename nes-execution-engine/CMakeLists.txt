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
set(CMAKE_VERBOSE_MAKEFILE on)

include(../cmake/LLVMIRUtil.cmake)

add_library(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Experimental/Interpreter/ProxyFunctions.cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/Experimental/Interpreter/PipelineContext.cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/Experimental/Runtime/RuntimePipelineContext.cpp ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/src/Runtime/detail/TupleBufferImpl.cpp)
target_include_directories(nes-runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(nes-runtime PUBLIC nes)
target_include_directories(nes-runtime PUBLIC ${CMAKE_BINARY_DIR}/include/nebulastream)
set_target_properties(nes-runtime PROPERTIES LINKER_LANGUAGE CXX)
llvmir_attach_bc_target(nes-runtime_bc nes-runtime -O3)
llvmir_attach_link_target(nes-runtime_link nes-runtime_bc)
llvmir_attach_opt_pass_target(nes-runtime_opt nes-runtime_link -S -O3)
get_target_property(OP_BINARY_DIR nes-runtime_opt LLVMIR_DIR)
get_target_property(OP_BINARY_LLVMIR_FILES nes-runtime_opt LLVMIR_FILES)
message(STATUS "CMAKE_CURRENT_SOURCE_DIR_EXECUTION: ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "OP_BINARY_DIR: ${OP_BINARY_DIR}")
message(STATUS "OP_BINARY_LLVMIR_FILES: ${OP_BINARY_LLVMIR_FILES}")

# Reducing linked & optimized LLVM IR to Proxy Functions only.
add_executable(generate-proxy-function-ir "${CMAKE_CURRENT_SOURCE_DIR}/src/Experimental/Utility/ExtractFunctionsFromLLVMIR.cpp")
set_target_properties( generate-proxy-function-ir PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${OP_BINARY_DIR}" )
target_link_libraries(generate-proxy-function-ir
    nes-execution-engine)
add_dependencies(generate-proxy-function-ir nes-runtime_opt)

add_custom_command(TARGET generate-proxy-function-ir POST_BUILD
    COMMAND "${OP_BINARY_DIR}/generate-proxy-function-ir"
    COMMAND "echo" ARGS "Executing function reducer on linked and optimized LLVM IR."
    VERBATIM)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-execution-engine NES_EXECUTION_ENGINE_SOURCE_FILES)

# Add Library
add_library(nes-execution-engine SHARED ${NES_EXECUTION_ENGINE_SOURCE_FILES})
message(STATUS "Here: ${NES_EXECUTION_ENGINE_SOURCE_FILES}")
target_link_libraries(nes-execution-engine PUBLIC nes)
target_include_directories(nes-execution-engine PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# include MLIR. This is currently required
find_package(MLIR REQUIRED CONFIG)
if (MLIR_FOUND)
    target_include_directories(nes-execution-engine SYSTEM PUBLIC
            $<BUILD_INTERFACE:${MLIR_INCLUDE_DIRS}>
            $<INSTALL_INTERFACE:include/nebulastream/>)
    add_compile_definitions(MLIR_COMPILER)
    get_property(mlir_libs GLOBAL PROPERTY MLIR_ALL_LIBS)
    message(STATUS "mlir_libs: ${mlir_libs}")
    target_link_libraries(nes-execution-engine PRIVATE ${mlir_libs} )
else ()
    message(FATAL_ERROR "Cannot find mlir")
endif (MLIR_FOUND)
message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-execution-engine
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)