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
target_link_libraries(nes-runtime PUBLIC nes-common nes-operators nes-data-types nes-memory nes-sources)

target_include_directories(nes-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# folly
include(CMakeFindDependencyMacro)
find_package(folly CONFIG REQUIRED)
target_link_libraries(nes-runtime PRIVATE folly::folly)
target_include_directories(nes-runtime PRIVATE ${FOLLY_INCLUDE_DIR})


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-runtime REUSE_FROM nes-common)
endif ()

target_compile_options(nes-runtime PUBLIC "-Wno-bitwise-instead-of-logical")
#target_compile_options(nes-runtime PUBLIC "-Wno-bitwise-instead-of-logical" "-fPIC")


# Add Private Dependencies
if (NES_ENABLES_TESTS AND NOT (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin"))
    # this allows to mock read calls
    target_compile_definitions(nes-runtime PRIVATE WRAP_READ_CALL)
    target_link_libraries(nes-runtime PRIVATE  "-Wl,--wrap,read")
endif ()

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_CPP)
    message(STATUS "Enable cpp compiler backend")
    target_link_libraries(nes-runtime PRIVATE nes-compiler)
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

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
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/BufferAccessException.hpp Runtime/MemoryLayout/BufferAccessException.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/MemoryLayout.hpp Runtime/MemoryLayout/MemoryLayout.hpp)

#Add the nes common and headers to cpack
install(TARGETS nes-runtime
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)
