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
#set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-runtime NES_EXECUTION_ENGINE_SOURCE_FILES)

# Add Library
add_library(nes-runtime ${NES_EXECUTION_ENGINE_SOURCE_FILES})
# if musl we cant use can't use backtrace for tracing
if (HOST_IS_MUSL)
    target_compile_definitions(nes-runtime PRIVATE HOST_IS_MUSL)
endif ()

target_link_libraries(nes-runtime PRIVATE nes-common nes-grpc nes-data-types nes-operators nes-configurations)
target_include_directories(nes-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# folly
if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    include(CMakeFindDependencyMacro)
    find_dependency(gflags CONFIG REQUIRED) # required on macos
    find_package(folly CONFIG REQUIRED)
    target_link_libraries(nes-runtime PRIVATE Folly::folly)
    target_include_directories(nes-runtime PRIVATE ${FOLLY_INCLUDE_DIR})
endif ()

# Library containing Kafka client
if (NES_USE_KAFKA)
    # TODO #3351 The commands should be moved into a separate CMake config file.
    if (NOT APPLE)
        find_package(cppkafka REQUIRED)
        target_link_libraries(nes-runtime PRIVATE cppkafka::cppkafka )
    else ()
        message(WARNING "ENABLE_KAFKA_BUILD Not supported on OSX")
    endif ()
endif ()

# Library containing MQTT client
if (NES_USE_MQTT)
    message("VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
    find_package(PahoMqttCpp CONFIG REQUIRED)
    target_link_libraries(nes-runtime PRIVATE PahoMqttCpp::paho-mqttpp3-static)
endif (NES_USE_MQTT)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-runtime REUSE_FROM nes-common)
endif ()

target_compile_options(nes-runtime PUBLIC "-Wno-bitwise-instead-of-logical")
#target_compile_options(nes-runtime PUBLIC "-Wno-bitwise-instead-of-logical" "-fPIC")


# Add Private Dependencies
target_link_libraries(nes-runtime PRIVATE ${LIBRARIES_ZMQ})


if (NES_ENABLE_EXPERIMENTAL_EXECUTION_CPP)
    message(STATUS "Enable cpp compiler backend")
    target_link_libraries(nes-runtime PRIVATE nes-compiler)
endif ()

if (NES_USE_GPU)
    target_link_libraries(nes-runtime PRIVATE ${LIBRARIES_CUDA})
endif ()

if (NES_USE_OPENCL)
    find_package(OpenCL REQUIRED)
    if (${OpenCL_FOUND})
        MESSAGE(STATUS "Found OpenCL ${OpenCL_VERSION_STRING}")
        MESSAGE(STATUS "OpenCL libraries: ${OpenCL_LIBRARIES}")
        MESSAGE(STATUS "OpenCL includes: ${OpenCL_INCLUDE_DIRS}")
        target_link_libraries(nes-runtime PRIVATE ${OpenCL_LIBRARIES})
        target_include_directories(nes-runtime PRIVATE ${OpenCL_INCLUDE_DIRS})
        target_compile_definitions(nes-runtime PRIVATE ENABLE_OPENCL)
    else ()
        message(FATAL_ERROR "Could not find OpenCL")
    endif ()
endif (NES_USE_OPENCL)

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_BYTECODE_INTERPRETER)
    message(STATUS "Enable dyncall")

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
