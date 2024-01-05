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

include(../cmake/LLVMIRUtil.cmake)
include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)

get_source(nes-execution NES_EXECUTION_SOURCE_FILES)

# Add Library
add_library(nes-execution ${NES_EXECUTION_SOURCE_FILES})
# Todo: figure out which libraries are necessary to link
target_link_libraries(nes-execution PRIVATE nes-common nes-compiler nes-grpc nes-data-types nes-operators nes-configurations nes-runtime)
# target_link_libraries(nes-execution PRIVATE nes-common nes-grpc nes-data-types nes-operators nes-configurations)
target_include_directories(nes-execution PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-execution REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-execution PUBLIC "-fPIC")
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

message(STATUS "Runtime benchmarks status: ${NES_RUNTIME_BENCHMARKS}")
if (NES_RUNTIME_BENCHMARKS)
    # Add tests with command
    add_subdirectory(benchmark)
    message(STATUS "Runtime benchmarks are enabled")
endif ()

# Add MLIR and LLVM dependencies
if (NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR)
    # include MLIR. This is currently required
    find_package(MLIR REQUIRED CONFIG)
    if (MLIR_FOUND)
        message(STATUS MLIR_INCLUDE_DIRS=${MLIR_INCLUDE_DIRS})
        target_include_directories(nes-execution SYSTEM PUBLIC
                $<BUILD_INTERFACE:${MLIR_INCLUDE_DIRS}>
                $<INSTALL_INTERFACE:include/nebulastream/>)
        add_compile_definitions(MLIR_COMPILER)
        get_property(mlir_libs GLOBAL PROPERTY MLIR_ALL_LIBS)
        get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
        get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)
        message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")
        message(DEBUG "mlir_libs: ${mlir_libs}")
        message(DEBUG "dialect_libs: ${dialect_libs}")
        message(DEBUG "conversion_libs: ${MLIR_INCLUDE_DIRS}")
        target_link_libraries(nes-execution PRIVATE ${mlir_libs} ${dialect_libs}
                ${conversion_libs}
                MLIRReconcileUnrealizedCasts
                MLIRMemRefToLLVM
                MLIRExecutionEngine)

        # Include necessary mlir library in the deb package and install it in the lib folder
        install(IMPORTED_RUNTIME_ARTIFACTS mlir_float16_utils mlir_runner_utils mlir_c_runner_utils mlir_async_runtime DESTINATION lib)

    else ()
        message(FATAL_ERROR "Cannot find mlir")
    endif (MLIR_FOUND)
    target_compile_definitions(nes-execution PRIVATE USE_MLIR)

    # If we want to support inlining we have to compile the llvm ir for the provided headers.
    # This is currently only supported on Ubuntu 22.04
    if (NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR_INLINING)
        add_library(nes-runtime-proxy ${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/src/Runtime/detail/TupleBufferImpl.cpp ${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/src/Execution/TupleBufferProxyFunctions.cpp)
        message(STATUS "CMAKE_SOURCE_DIR ${CMAKE_SOURCE_DIR}")
        target_include_directories(nes-runtime-proxy PUBLIC ${CMAKE_SOURCE_DIR}/nes-common/include/ ${CMAKE_SOURCE_DIR}/nes-common/include/ ${CMAKE_SOURCE_DIR}/nes-runtime/include/ ${CMAKE_SOURCE_DIR}/nes-data-types/include/)

        set_target_properties(nes-runtime-proxy PROPERTIES LINKER_LANGUAGE CXX)
        llvmir_attach_bc_target(nes-runtime_bc nes-runtime-proxy -O3)
        add_dependencies(nes-runtime_bc nes-runtime-proxy)

        llvmir_attach_link_target(nes-runtime_link nes-runtime_bc)
        llvmir_attach_opt_pass_target(nes-runtime_opt nes-runtime_link -S -O3)

        get_target_property(OP_BINARY_DIR nes-runtime_opt LLVMIR_DIR)
        get_target_property(OP_BINARY_LLVMIR_FILES nes-runtime_opt LLVMIR_FILES)

        message(STATUS "CMAKE_CURRENT_SOURCE_DIR_EXECUTION: ${CMAKE_CURRENT_SOURCE_DIR}")
        message(STATUS "OP_BINARY_DIR: ${OP_BINARY_DIR}")
        message(STATUS "OP_BINARY_LLVMIR_FILES: ${OP_BINARY_LLVMIR_FILES}")

        # Reducing linked & optimized LLVM IR to Proxy Functions only.
        add_executable(generate-proxy-function-ir "${CMAKE_CURRENT_SOURCE_DIR}/src/Experimental/Utility/ExtractFunctionsFromLLVMIR.cpp")
        set_target_properties(generate-proxy-function-ir PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${OP_BINARY_DIR}")
        list(POP_FRONT OP_BINARY_LLVMIR_FILES OP_BINARY_LLVMIR_FILE)
        target_compile_definitions(nes-runtime-proxy PRIVATE PROXY_FUNCTIONS_RESULT_DIR_CMAKE=\"${OP_BINARY_DIR}\")

        target_compile_definitions(generate-proxy-function-ir PRIVATE IR_FILE_DIR=\"${OP_BINARY_DIR}\")
        target_compile_definitions(generate-proxy-function-ir PRIVATE IR_FILE_FILE=\"${OP_BINARY_LLVMIR_FILE}\")
        target_compile_definitions(generate-proxy-function-ir PRIVATE PROXY_FUNCTIONS_RESULT_DIR_CMAKE=\"${OP_BINARY_DIR}\")

        target_link_libraries(generate-proxy-function-ir ${mlir_libs})
        target_link_libraries(generate-proxy-function-ir ${llvm_libs})
        target_include_directories(generate-proxy-function-ir SYSTEM PRIVATE ${LLVM_INCLUDE_DIRS})

        add_custom_command(TARGET generate-proxy-function-ir POST_BUILD
                COMMAND "${OP_BINARY_DIR}/generate-proxy-function-ir"
                COMMAND "echo" ARGS "Executing function reducer on linked and optimized LLVM IR."
                VERBATIM)
        add_dependencies(generate-proxy-function-ir nes-runtime_opt)
        add_dependencies(nes-runtime-proxy generate-proxy-function-ir)
    endif ()
endif ()


if (NES_ENABLE_EXPERIMENTAL_EXECUTION_FLOUNDER)
    # add flounder backend
    target_compile_definitions(nes-runtime PUBLIC USE_FLOUNDER)
    include(FetchContent)
    FetchContent_Declare(
            flounder
            GIT_REPOSITORY git@github.com:nebulastream/flounder-compiler.git
            GIT_TAG flounder
            PREFIX ${BOTAN_LIB_FCSTUFF_DIR}
            SOURCE_DIR "lib/flounder"
            BINARY_DIR "lib/flounder"
    )
    set(flounder_BUILD_TESTING OFF CACHE INTERNAL "Turn off tests")
    set(FETCHCONTENT_QUIET OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(flounder)
    target_compile_options(flounder PRIVATE -Wno-unqualified-std-cast-call)
    target_link_libraries(nes-runtime PRIVATE flounder)
    target_include_directories(nes-runtime PRIVATE ${flounder_SOURCE_DIR}/src)
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    # add_subdirectory(tests) #Todo: enable again
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_JNI)
    target_link_libraries(nes-runtime PUBLIC ${JNI_LIBRARIES})
    # Define map java udf utils class file
    SET(NES_JAVA_UTILS_SRC_PATH "src/Execution/Operators/Relational/JavaUDF")
    if (NES_JAVA_UDF_UTILS_PATH)
        # Use explicit path to Java helpers when building Debian package for the nes-executable-image Docker image.
        SET(JAVA_UDF_UTILS_JAR "${CMAKE_INSTALL_PREFIX}/${NES_JAVA_UDF_UTILS_PATH}/JavaUtils.jar")
        target_compile_definitions(nes-runtime PUBLIC JAVA_UDF_UTILS_JAR="${JAVA_UDF_UTILS_JAR}")
        message(STATUS "Setting JAR with Java helpers to: ${JAVA_UDF_UTILS_JAR}")
        # Install Java Utils JAR
        INSTALL(FILES "${CMAKE_CURRENT_BINARY_DIR}/${NES_JAVA_UTILS_SRC_PATH}/JavaUtils.jar" DESTINATION ${NES_JAVA_UDF_UTILS_PATH})
    else ()
        # Use Java helpers from test directory when working in CLion.
        target_compile_definitions(nes-runtime PUBLIC JAVA_UDF_UTILS_JAR="${CMAKE_CURRENT_BINARY_DIR}/${NES_JAVA_UTILS_SRC_PATH}/JavaUtils.jar")
    endif()
endif()


#Add the nes common and headers to cpack
install(TARGETS nes-execution
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)