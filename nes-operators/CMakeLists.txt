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

# Set Sources and Headers
add_subdirectory(src)

get_source(nes-operators NES_OPERATORS_SOURCE_FILES)

# Add Library
add_library(nes-operators ${NES_OPERATORS_SOURCE_FILES})
target_link_libraries(nes-operators PRIVATE nes-common nes-configurations nes-data-types nes-grpc)
target_include_directories(nes-operators PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Library containing OPC client
if (NES_USE_OPC)
    find_package(open62541 CONFIG REQUIRED)
    target_link_libraries(nes-operators PUBLIC open62541::open62541)
    add_compile_definitions(ENABLE_OPC_BUILD)
endif (NES_USE_OPC)

register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/Expressions/ExpressionNode.hpp Operators/Expressions/ExpressionNode.hpp)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/Expressions/FieldAccessExpressionNode.hpp Operators/Expressions/FieldAccessExpressionNode.hpp)
register_public_header_file(nes-operators include/Operators/LogicalOperators/Operators/BatchJoinDescriptor.hpp Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp)
register_public_header_dir(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Windows/ Operators/LogicalOperators/Windows/)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Plans/Query/QueryPlan.hpp Plans/Query/QueryPlan.hpp)
register_public_header_file(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Network/NesPartition.hpp Operators/LogicalOperators/Network/NesPartition.hpp)
register_public_header_dir(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Sources Operators/LogicalOperators/Sources)
register_public_header_dir(nes-operators ${CMAKE_CURRENT_SOURCE_DIR}/include/Operators/LogicalOperators/Sinks Operators/LogicalOperators/Sinks)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-operators
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)