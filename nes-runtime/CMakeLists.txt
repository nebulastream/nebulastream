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



macro(target_public_headers TARGET)
    set_target_properties(${TARGET} PROPERTIES PUBLIC_HEADER "${ARGN}")
endmacro()

# Add nes-core library
get_source(nes-runtime NES_RUNTIME_SOURCE_FILES)
message(STATUS "runtime has ${NES_RUNTIME_SOURCE_FILES}")

add_library(nes-runtime SHARED ${NES_RUNTIME_SOURCE_FILES})
#target_sources(nes-runtime PUBLIC )
target_include_directories(nes-runtime PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
target_link_libraries(nes-runtime PUBLIC nes-common nes-compiler nes-core)


get_target_property(LIBA_INCLUDES nes-runtime INCLUDE_DIRECTORIES)
foreach (dir ${LIBA_INCLUDES})
    string(APPEND INCLUDE_COMPILER_STRING "/I${dir} ")
    message(STATUS "NES Runtime includes ${dir}")
endforeach ()

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")

# Select fine-grained headers
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/API/ API)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Common/DataTypes Common/DataTypes)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Windowing Windowing)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Sources Operators/LogicalOperators/Sources)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Sinks Operators/LogicalOperators/Sinks)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Common/ExecutableType Common/ExecutableType)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Common/PhysicalTypes Common/PhysicalTypes)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/ QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Execution/ Runtime/Execution/)
register_public_header_dir(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/State/ State/)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/QueryCompiler/QueryCompilerForwardDeclaration.hpp QueryCompiler/QueryCompilerForwardDeclaration.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Reconfigurable.hpp Runtime/Reconfigurable.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/ReconfigurationType.hpp Runtime/ReconfigurationType.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/ReconfigurationMessage.hpp Runtime/ReconfigurationMessage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Plans/Query/QueryId.hpp Plans/Query/QueryId.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Plans/Query/QuerySubPlanId.hpp Plans/Query/QuerySubPlanId.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/NesThread.hpp Runtime/NesThread.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/RuntimeForwardRefs.hpp Runtime/RuntimeForwardRefs.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/UtilityFunctions.hpp Util/UtilityFunctions.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/QueryTerminationType.hpp Runtime/QueryTerminationType.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/OperatorId.hpp Operators/OperatorId.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Plans/Query/QueryPlan.hpp Plans/Query/QueryPlan.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/TupleBuffer.hpp Runtime/TupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Network/NesPartition.hpp Network/NesPartition.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/BufferStorage.hpp Runtime/BufferStorage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/AbstractBufferStorage.hpp Runtime/AbstractBufferStorage.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/detail/TupleBufferImpl.hpp Runtime/detail/TupleBufferImpl.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/WorkerContext.hpp Runtime/WorkerContext.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/ExecutionResult.hpp Runtime/ExecutionResult.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Network/NetworkForwardRefs.hpp Network/NetworkForwardRefs.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Nodes/Expressions/FieldAccessExpressionNode.hpp Nodes/Expressions/FieldAccessExpressionNode.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Nodes/Expressions/ExpressionNode.hpp Nodes/Expressions/ExpressionNode.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include/Nodes/Node.hpp Nodes/Node.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/DynamicTupleBuffer.hpp Runtime/MemoryLayout/DynamicTupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/RowLayout.hpp Runtime/MemoryLayout/RowLayout.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/RowLayoutField.hpp Runtime/MemoryLayout/RowLayoutField.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/ColumnLayout.hpp Runtime/MemoryLayout/ColumnLayout.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/BufferManager.hpp Runtime/BufferManager.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/AbstractBufferProvider.hpp Runtime/AbstractBufferProvider.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Allocator/NesDefaultMemoryAllocator.hpp Runtime/Allocator/NesDefaultMemoryAllocator.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/BufferRecycler.hpp Runtime/BufferRecycler.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/HashMap.hpp Util/Experimental/HashMap.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/Hash.hpp Util/Experimental/Hash.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/CRC32Hash.hpp Util/Experimental/CRC32Hash.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/MurMurHash3.hpp Util/Experimental/MurMurHash3.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Exceptions/WindowProcessingException.hpp Exceptions/WindowProcessingException.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Exceptions/BufferAccessException.hpp Exceptions/BufferAccessException.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/MemoryLayout/MemoryLayout.hpp Runtime/MemoryLayout/MemoryLayout.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/FaultToleranceType.hpp Util/FaultToleranceType.hpp)
register_public_header_file(nes-runtime ../nes-core/include/Monitoring/MetricCollectors/MetricCollectorType.hpp Monitoring/MetricCollectors/MetricCollectorType.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/LineageType.hpp Util/LineageType.hpp)
register_public_header_file(nes-runtime ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/OriginId.hpp Operators/OriginId.hpp)


if (NES_ENABLES_TESTS)
    # Add tests with command
#    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

##Add the nes lib and headers to cpack
install(TARGETS nes-runtime
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)

install(DIRECTORY ${CMAKE_BINARY_DIR}/include/nebulastream
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY ${CLANG_INCLUDE_DIR}
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_BINDIR}/lib/clang/${LLVM_PACKAGE_VERSION}/include/)
