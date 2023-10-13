/*
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

#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/CEP/IterationLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LimitLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameSourceOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/NonKeyedWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windows/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>

namespace NES {

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createSourceOperator(const SourceDescriptorPtr& sourceDescriptor, OperatorId id, OriginId originId) {
    return std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor, id, originId);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createSinkOperator(const SinkDescriptorPtr& sinkDescriptor, OperatorId id) {
    return std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createFilterOperator(const ExpressionNodePtr& predicate, OperatorId id) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createRenameSourceOperator(const std::string& newSourceName, OperatorId id) {
    return std::make_shared<RenameSourceOperatorNode>(newSourceName, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createLimitOperator(const uint64_t limit, OperatorId id) {
    return std::make_shared<LimitLogicalOperatorNode>(limit, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createProjectionOperator(const std::vector<ExpressionNodePtr>& expressions,
                                                                             OperatorId id) {
    return std::make_shared<ProjectionLogicalOperatorNode>(expressions, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createMapOperator(const FieldAssignmentExpressionNodePtr& mapExpression,
                                                                      OperatorId id) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createInferModelOperator(std::string model,
                                                                             std::vector<ExpressionItemPtr> inputFieldsPtr,
                                                                             std::vector<ExpressionItemPtr> outputFieldsPtr,
                                                                             OperatorId id) {

    return std::make_shared<NES::InferModel::InferModelLogicalOperatorNode>(model, inputFieldsPtr, outputFieldsPtr, id);
}

LogicalBinaryOperatorNodePtr LogicalOperatorFactory::createUnionOperator(OperatorId id) {
    return std::make_shared<UnionLogicalOperatorNode>(id);
}

LogicalBinaryOperatorNodePtr LogicalOperatorFactory::createJoinOperator(const Join::LogicalJoinDefinitionPtr& joinDefinition,
                                                                        OperatorId id) {
    return std::make_shared<JoinLogicalOperatorNode>(joinDefinition, id);
}

LogicalBinaryOperatorNodePtr
LogicalOperatorFactory::createBatchJoinOperator(const Join::Experimental::LogicalBatchJoinDefinitionPtr& batchJoinDefinition,
                                                OperatorId id) {
    return std::make_shared<Experimental::BatchJoinLogicalOperatorNode>(batchJoinDefinition, id);
}

BroadcastLogicalOperatorNodePtr LogicalOperatorFactory::createBroadcastOperator(OperatorId id) {
    return std::make_shared<BroadcastLogicalOperatorNode>(id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createWindowOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                               OperatorId id) {
    return std::make_shared<NonKeyedWindowOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                               OperatorId id) {
    return std::make_shared<SliceCreationOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                                   OperatorId id) {
    return std::make_shared<WindowComputationOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                              OperatorId id) {
    return std::make_shared<SliceMergingOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createWatermarkAssignerOperator(
    const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor,
    OperatorId id) {
    return std::make_shared<WatermarkAssignerLogicalOperatorNode>(watermarkStrategyDescriptor, id);
}
LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createCEPIterationOperator(const uint64_t minIterations, const uint64_t maxIterations, OperatorId id) {
    return std::make_shared<IterationLogicalOperatorNode>(minIterations, maxIterations, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id) {
    return std::make_shared<MapUDFLogicalOperatorNode>(udfDescriptor, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createFlatMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id) {
    return std::make_shared<FlatMapUDFLogicalOperatorNode>(udfDescriptor, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createOpenCLLogicalOperator(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor, OperatorId id) {
    return std::make_shared<OpenCLLogicalOperatorNode>(javaUdfDescriptor, id);
}

}// namespace NES
