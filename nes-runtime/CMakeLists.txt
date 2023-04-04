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

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-runtime NES_EXECUTION_ENGINE_SOURCE_FILES)

# Add Library
add_library(nes-runtime SHARED ${NES_EXECUTION_ENGINE_SOURCE_FILES})
# if musl we cant use can't use backtrace for tracing
if (HOST_IS_MUSL)
    target_compile_definitions(nes-runtime PRIVATE HOST_IS_MUSL)
endif ()

target_link_libraries(nes-runtime PUBLIC nes-common nes-data-types)
target_include_directories(nes-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_compile_options(nes-runtime PUBLIC "-Wno-bitwise-instead-of-logical" "-fPIC")

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_CPP)
    message(STATUS "Enable cpp compiler backend")
    target_link_libraries(nes-runtime PRIVATE nes-compiler)
endif ()

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_BYTECODE_INTERPRETER)
    message(STATUS "Enable bytecode interpreter")


    # We use the dyncall lib to call arbitrary runtime function.
    # Set the path to the dyncall directory
    set(dyncall_dir "${CMAKE_CURRENT_BINARY_DIR}/lib/dyncall")

    # Create the include directory for dyncall
    file(MAKE_DIRECTORY ${dyncall_dir}/include)

    # Download and build the external project
    ExternalProject_Add(dyncallProject
            GIT_REPOSITORY "https://github.com/nebulastream/dyncall"
            GIT_TAG "origin/main"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR> # Specify installation directory
            BUILD_IN_SOURCE true # Build the project in the source directory
            BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libdyncall_s.a # Specify path to the library file that will be built
            INSTALL_DIR ${dyncall_dir} # Install dyncall to the specified directory
            UPDATE_DISCONNECTED 1
            )

    # Create a static imported library for the external project
    add_library(dyncall STATIC IMPORTED)
    add_dependencies(dyncall dyncallProject)

    # Get the installation directory of the external project
    ExternalProject_Get_Property(dyncallProject install_dir)

    # Add the include directory of the external project to the interface of extLib
    target_include_directories(dyncall INTERFACE ${install_dir}/include)

    # Set the imported location of the external library to the installation directory
    set_target_properties(dyncall PROPERTIES IMPORTED_LOCATION ${install_dir}/lib/libdyncall_s.a)

    # Link the external library to nes-runtime
    target_link_libraries(nes-runtime PRIVATE dyncall)

endif ()

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR)
    # include MLIR. This is currently required
    find_package(MLIR REQUIRED CONFIG)
    if (MLIR_FOUND)
        message(STATUS MLIR_INCLUDE_DIRS=${MLIR_INCLUDE_DIRS})
        target_include_directories(nes-runtime SYSTEM PUBLIC
                $<BUILD_INTERFACE:${MLIR_INCLUDE_DIRS}>
                $<INSTALL_INTERFACE:include/nebulastream/>)
        add_compile_definitions(MLIR_COMPILER)
        get_property(mlir_libs GLOBAL PROPERTY MLIR_ALL_LIBS)
        get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
        get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)
        message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")
        message(STATUS "mlir_libs: ${mlir_libs}")
        message(STATUS "dialect_libs: ${dialect_libs}")
        message(STATUS "conversion_libs: ${MLIR_INCLUDE_DIRS}")
        target_link_libraries(nes-runtime PRIVATE ${mlir_libs} ${dialect_libs}
                ${conversion_libs}
                MLIRReconcileUnrealizedCasts
                MLIRMemRefToLLVM
                MLIRExecutionEngine)

        # Include necessary mlir library in the deb package and install it in the lib folder
        install(IMPORTED_RUNTIME_ARTIFACTS mlir_float16_utils mlir_runner_utils mlir_c_runner_utils mlir_async_runtime DESTINATION lib)

    else ()
        message(FATAL_ERROR "Cannot find mlir")
    endif (MLIR_FOUND)
    target_compile_definitions(nes-runtime PRIVATE USE_MLIR)

    # If we want to support inlining we have to compile the llvm ir for the provided headers.
    # This is currently only supported on Ubuntu 22.04
    if (NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR_INLINING)
        add_library(nes-runtime-proxy ${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/src/Runtime/detail/TupleBufferImpl.cpp ${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/src/Execution/TupleBufferProxyFunctions.cpp)
        message(STATUS "CMAKE_SOURCE_DIR ${CMAKE_SOURCE_DIR}")
        target_include_directories(nes-runtime-proxy PUBLIC ${CMAKE_SOURCE_DIR}/nes-core/include/ ${CMAKE_SOURCE_DIR}/nes-common/include/ ${CMAKE_SOURCE_DIR}/nes-runtime/include/ ${CMAKE_SOURCE_DIR}/nes-data-types/include/)

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

if ((NES_ENABLE_EXPERIMENTAL_EXECUTION_JNI) AND (NES_ENABLE_EXPERIMENTAL_EXECUTION_BABELFISH))
    message(FATAL_ERROR "You cant activate JNI and BABELFISH at the same time")
elseif (NES_ENABLE_EXPERIMENTAL_EXECUTION_BABELFISH)
    target_compile_definitions(nes-runtime PUBLIC USE_BABELFISH)
    add_library(babelfish STATIC IMPORTED)
    set_target_properties(babelfish PROPERTIES IMPORTED_LOCATION //home/pgrulich/projects/nes/nautilusbf/makenative/lib/babelfish.so)
    set_target_properties(babelfish PROPERTIES INTERFACE_INCLUDE_DIRECTORIES /home/pgrulich/projects/nes/nautilusbf/makenative/lib/)
    target_link_libraries(nes-runtime PUBLIC babelfish)
    target_include_directories(nes-runtime PUBLIC
            $<BUILD_INTERFACE:/home/pgrulich/projects/nes/nautilusbf/makenative/lib/>
            $<INSTALL_INTERFACE:/home/pgrulich/projects/nes/nautilusbf/makenative/lib/>)
endif ()

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_FLOUNDER)
    target_compile_definitions(nes-runtime PUBLIC USE_FLOUNDER)
    #ExternalProject_Add(
    #        fmt-external
    #        GIT_REPOSITORY "https://github.com/fmtlib/fmt"
    #        GIT_TAG "8.1.1"
    #        PREFIX "lib/fmt"
    #        INSTALL_COMMAND cmake -E echo ""
    #        CMAKE_ARGS "-DBUILD_SHARED_LIBS=TRUE -DFMT_TEST=FALSE -DFMT_MASTER_PROJECT=OFF"
   # )

    target_include_directories(nes-runtime PUBLIC
            $<BUILD_INTERFACE:/home/pgrulich/projects/MxTasking/src/>
            $<INSTALL_INTERFACE:/home/pgrulich/projects/MxTasking/src/>)

    target_include_directories(nes-runtime PUBLIC
            $<BUILD_INTERFACE:/home/pgrulich/projects/MxTasking/lib/asmjit/src/>
            $<INSTALL_INTERFACE:/home/pgrulich/projects/MxTasking/lib/asmjit/src/>)
    target_link_libraries(nes-runtime PUBLIC /home/pgrulich/projects/MxTasking/lib/libflounder.a)
    target_link_libraries(nes-runtime PUBLIC /home/pgrulich/projects/MxTasking/lib/libasmjit.a)
    #find_package(fmt REQUIRED)
    target_link_libraries(nes-runtime PUBLIC fmt::fmt-header-only)
    #target_link_libraries(nes-runtime PUBLIC fmt::fmt)
    #target_link_libraries(nes-runtime PUBLIC /home/pgrulich/projects/MxTasking/lib/fmt/src/fmt-external-build/libfmt.so.9)
endif ()

if (NES_RUNTIME_BENCHMARKS)
    # Add tests with command
    add_subdirectory(benchmark)
    message(STATUS "Runtime benchmarks are enabled")
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

# Define map java udf utils class file
add_compile_definitions(JAVA_UDF_UTILS="${CMAKE_CURRENT_BINARY_DIR}/test_data/JavaUdfTestData/")

register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/DynamicTupleBuffer.hpp Runtime/MemoryLayout/DynamicTupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/RowLayout.hpp Runtime/MemoryLayout/RowLayout.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/RowLayoutField.hpp Runtime/MemoryLayout/RowLayoutField.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/ColumnLayout.hpp Runtime/MemoryLayout/ColumnLayout.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/BufferManager.hpp Runtime/BufferManager.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/AbstractBufferProvider.hpp Runtime/AbstractBufferProvider.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Allocator/NesDefaultMemoryAllocator.hpp Runtime/Allocator/NesDefaultMemoryAllocator.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/BufferRecycler.hpp Runtime/BufferRecycler.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/NesThread.hpp Runtime/NesThread.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/RuntimeForwardRefs.hpp Runtime/RuntimeForwardRefs.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/QueryTerminationType.hpp Runtime/QueryTerminationType.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Reconfigurable.hpp Runtime/Reconfigurable.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/ReconfigurationType.hpp Runtime/ReconfigurationType.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/ReconfigurationMessage.hpp Runtime/ReconfigurationMessage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/TupleBuffer.hpp Runtime/TupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/TaggedPointer.hpp Runtime/TaggedPointer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Network/NesPartition.hpp Network/NesPartition.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/BufferStorage.hpp Runtime/BufferStorage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/AbstractBufferStorage.hpp Runtime/AbstractBufferStorage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/detail/TupleBufferImpl.hpp Runtime/detail/TupleBufferImpl.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/WorkerContext.hpp Runtime/WorkerContext.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Execution/OperatorHandler.hpp Runtime/Execution/OperatorHandler.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Execution/ExecutablePipelineStage.hpp Runtime/Execution/ExecutablePipelineStage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Execution/PipelineExecutionContext.hpp Runtime/Execution/PipelineExecutionContext.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/ExecutionResult.hpp Runtime/ExecutionResult.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Allocator/MemoryResource.hpp Runtime/Allocator/MemoryResource.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Network/NetworkForwardRefs.hpp Network/NetworkForwardRefs.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/BufferAccessException.hpp Runtime/MemoryLayout/BufferAccessException.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/MemoryLayout.hpp Runtime/MemoryLayout/MemoryLayout.hpp)

#Add the nes common and headers to cpack
install(TARGETS nes-runtime
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)
