/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Expressions/Expressions.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>

namespace NES {

UnaryOperatorNodePtr LogicalOperatorFactory::createSourceOperator(const SourceDescriptorPtr sourceDescriptor, OperatorId id) {
    return std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor, id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createSinkOperator(const SinkDescriptorPtr sinkDescriptor, OperatorId id) {
    return std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createFilterOperator(const ExpressionNodePtr predicate, OperatorId id) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate, id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createProjectionOperator(std::vector<ExpressionItem> expressions, OperatorId id) {
    return std::make_shared<ProjectionLogicalOperatorNode>(expressions, id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createMapOperator(const FieldAssignmentExpressionNodePtr mapExpression,
                                                                 OperatorId id) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression, id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createMergeOperator(OperatorId id) {
    return std::make_shared<MergeLogicalOperatorNode>(id);
}

BinaryOperatorNodePtr LogicalOperatorFactory::createJoinOperator(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id) {
    return std::make_shared<JoinLogicalOperatorNode>(joinDefinition, id);
}

ExchangeOperatorNodePtr LogicalOperatorFactory::createBroadcastOperator(OperatorId id) {
    return std::make_shared<BroadcastLogicalOperatorNode>(id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                                    OperatorId id) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinition, id);
}

UnaryOperatorNodePtr
LogicalOperatorFactory::createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                               OperatorId id) {
    return std::make_shared<CentralWindowOperator>(windowDefinition, id);
}

UnaryOperatorNodePtr
LogicalOperatorFactory::createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                               OperatorId id) {
    return std::make_shared<SliceCreationOperator>(windowDefinition, id);
}

UnaryOperatorNodePtr
LogicalOperatorFactory::createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                                   OperatorId id) {
    return std::make_shared<WindowComputationOperator>(windowDefinition, id);
}

UnaryOperatorNodePtr
LogicalOperatorFactory::createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                              OperatorId id) {
    return std::make_shared<SliceMergingOperator>(windowDefinition, id);
}

UnaryOperatorNodePtr LogicalOperatorFactory::createWatermarkAssignerOperator(
    const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor, OperatorId id) {
    return std::make_shared<WatermarkAssignerLogicalOperatorNode>(watermarkStrategyDescriptor, id);
}

}// namespace NES
